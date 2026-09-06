//! Front plane: TLS termination (when configured) + byte proxy to the
//! business plane on loopback. Owns no cache semantics — it only moves
//! bytes. TLS material comes from `tls_cert_env` / `tls_key_env` paths;
//! absent material = plaintext proxy + boot warning (EdgeOne is expected
//! to carry public HTTPS in that deployment).

use std::{net::SocketAddr, path::Path, sync::Arc};

use anyhow::Context;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio_rustls::{
    rustls::{
        self,
        pki_types::{CertificateDer, PrivateKeyDer},
    },
    TlsAcceptor,
};
use tracing::{info, warn};

/// Build a TLS acceptor from PEM files (certificate chain + private key).
/// Fail-fast at boot: misconfigured material must never become a silent
/// plaintext downgrade.
pub fn load_acceptor(cert_file: &Path, key_file: &Path) -> anyhow::Result<TlsAcceptor> {
    let certs: Vec<CertificateDer> = rustls_pemfile::certs(
        &mut std::io::BufReader::new(std::fs::File::open(cert_file).with_context(|| format!("open tls cert {}", cert_file.display()))?),
    )
    .collect::<Result<_, _>>()
    .context("parse tls cert chain")?;
    if certs.is_empty() {
        anyhow::bail!("tls cert {} holds no certificates", cert_file.display());
    }
    let key: PrivateKeyDer = rustls_pemfile::private_key(
        &mut std::io::BufReader::new(std::fs::File::open(key_file).with_context(|| format!("open tls key {}", key_file.display()))?),
    )
    .context("parse tls key")?
    .ok_or_else(|| anyhow::anyhow!("tls key {} holds no private key", key_file.display()))?;
    let provider = Arc::new(rustls::crypto::aws_lc_rs::default_provider());
    let config = rustls::ServerConfig::builder_with_provider(provider)
        .with_protocol_versions(&[&rustls::version::TLS13, &rustls::version::TLS12])
        .context("tls protocol versions")?
        .with_no_client_auth()
        .with_single_cert(certs, key)
        .context("tls single cert")?;
    Ok(TlsAcceptor::from(Arc::new(config)))
}

/// Resolve TLS material from config env pointers. `Ok(None)` = plaintext
/// proxy (warned, not failed — loopback/edge-terminated deployments).
pub fn acceptor_from_env(
    tls_cert_env: Option<&str>,
    tls_key_env: Option<&str>,
) -> anyhow::Result<Option<TlsAcceptor>> {
    match (tls_cert_env, tls_key_env) {
        (Some(cert_env), Some(key_env)) => {
            let cert_path = std::env::var(cert_env)
                .with_context(|| format!("env {cert_env} not set"))?;
            let key_path = std::env::var(key_env)
                .with_context(|| format!("env {key_env} not set"))?;
            info!(cert = %cert_path, "front plane TLS enabled");
            Ok(Some(load_acceptor(Path::new(&cert_path), Path::new(&key_path))?))
        }
        (None, None) => {
            warn!("front plane without TLS (plaintext proxy) — terminate HTTPS at the edge");
            Ok(None)
        }
        _ => anyhow::bail!("tls_cert_env and tls_key_env must be set together"),
    }
}

pub async fn run_front(
    front: SocketAddr,
    business: SocketAddr,
    tls: Option<TlsAcceptor>,
    shutdown: impl std::future::Future<Output = ()> + Send + 'static,
) -> anyhow::Result<()> {
    let listener = TcpListener::bind(front).await?;
    info!(%front, business = %business, tls = tls.is_some(), "front plane listening");
    tokio::pin!(shutdown);
    loop {
        tokio::select! {
            _ = &mut shutdown => {
                info!("front plane shutting down");
                return Ok(());
            }
            accepted = listener.accept() => {
                let (inbound, peer) = accepted?;
                let tls = tls.clone();
                tokio::spawn(async move {
                    let r: anyhow::Result<()> = async {
                        match tls {
                            Some(acceptor) => {
                                let mut stream = acceptor.accept(inbound).await.context("tls accept")?;
                                proxy_one(&mut stream, business).await
                            }
                            None => {
                                let mut stream = inbound;
                                proxy_one(&mut stream, business).await
                            }
                        }
                    }
                    .await;
                    if let Err(e) = r {
                        warn!(%peer, error = %e, "proxy error");
                    }
                });
            }
        }
    }
}

async fn proxy_one<S>(inbound: &mut S, business: SocketAddr) -> anyhow::Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let mut buf = vec![0u8; 8192];
    let n = inbound.read(&mut buf).await?;
    if n == 0 {
        return Ok(());
    }
    let mut outbound = TcpStream::connect(business).await?;
    outbound.write_all(&buf[..n]).await?;
    tokio::io::copy_bidirectional(inbound, &mut outbound).await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    use std::path::PathBuf;

    fn self_signed_pair() -> (Vec<u8>, Vec<u8>) {
        let certified =
            rcgen::generate_simple_self_signed(vec!["localhost".into(), "127.0.0.1".into()]).unwrap();
        (certified.cert.pem().into_bytes(), certified.key_pair.serialize_pem().into_bytes())
    }

    fn write_pair(dir: &std::path::Path) -> (PathBuf, PathBuf) {
        let (cert, key) = self_signed_pair();
        let cert_path = dir.join("cert.pem");
        let key_path = dir.join("key.pem");
        std::fs::write(&cert_path, cert).unwrap();
        std::fs::write(&key_path, key).unwrap();
        (cert_path, key_path)
    }

    #[test]
    fn acceptor_loads_matching_pair() {
        let dir = tempfile::tempdir().unwrap();
        let (cert, key) = write_pair(dir.path());
        assert!(load_acceptor(&cert, &key).is_ok());
    }

    #[test]
    fn acceptor_rejects_missing_and_mismatched() {
        let dir = tempfile::tempdir().unwrap();
        let missing = dir.path().join("nope.pem");
        assert!(load_acceptor(&missing, &missing).is_err());
        // Cert as key and vice versa: parse failure, not silent plaintext.
        let (cert, key) = write_pair(dir.path());
        assert!(load_acceptor(&key, &cert).is_err());
    }

    #[test]
    fn env_pairing_rules() {
        // Neither set: plaintext, warned.
        assert!(acceptor_from_env(None, None).unwrap().is_none());
        // Half set: boot error (never a silent downgrade).
        std::env::set_var("FRONT_TEST_ONLY_CERT", "/tmp/x");
        assert!(acceptor_from_env(Some("FRONT_TEST_ONLY_CERT"), None).is_err());
        assert!(acceptor_from_env(None, Some("FRONT_TEST_ONLY_CERT")).is_err());
        // Both set, files valid: acceptor.
        let dir = tempfile::tempdir().unwrap();
        let (cert, key) = write_pair(dir.path());
        std::env::set_var("FRONT_TEST_CERT", cert.to_str().unwrap());
        std::env::set_var("FRONT_TEST_KEY", key.to_str().unwrap());
        assert!(acceptor_from_env(Some("FRONT_TEST_CERT"), Some("FRONT_TEST_KEY")).unwrap().is_some());
        std::env::remove_var("FRONT_TEST_ONLY_CERT");
        std::env::remove_var("FRONT_TEST_CERT");
        std::env::remove_var("FRONT_TEST_KEY");
    }

    /// Test-only verifier: accepts anything (loopback test). The assertion
    /// under test is termination + proxying, not PKI.
    #[derive(Debug)]
    struct AcceptAny;

    impl rustls::client::danger::ServerCertVerifier for AcceptAny {
        fn verify_server_cert(
            &self,
            _end_entity: &CertificateDer<'_>,
            _intermediates: &[CertificateDer<'_>],
            _server_name: &rustls::pki_types::ServerName<'_>,
            _ocsp: &[u8],
            _now: rustls::pki_types::UnixTime,
        ) -> Result<rustls::client::danger::ServerCertVerified, rustls::Error> {
            Ok(rustls::client::danger::ServerCertVerified::assertion())
        }

        fn verify_tls12_signature(
            &self,
            _message: &[u8],
            _cert: &CertificateDer<'_>,
            _dss: &rustls::DigitallySignedStruct,
        ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
            Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
        }

        fn verify_tls13_signature(
            &self,
            _message: &[u8],
            _cert: &CertificateDer<'_>,
            _dss: &rustls::DigitallySignedStruct,
        ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
            Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
        }

        fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
            use rustls::SignatureScheme::*;
            vec![
                ECDSA_NISTP256_SHA256,
                ECDSA_NISTP384_SHA384,
                ED25519,
                RSA_PSS_SHA256,
                RSA_PKCS1_SHA256,
            ]
        }
    }

    /// Full loop: TLS client → front (TLS) → plaintext business mock.
    /// Verifier accepts anything here (loopback test): the assertion under
    /// test is termination + proxying, not PKI.
    #[tokio::test]
    async fn tls_front_proxies_to_business() {
        let dir = tempfile::tempdir().unwrap();
        let (cert_path, key_path) = write_pair(dir.path());
        let acceptor = load_acceptor(&cert_path, &key_path).unwrap();

        // Business mock: read request head, reply canned bytes.
        let business = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let business_addr = business.local_addr().unwrap();
        tokio::spawn(async move {
            let (mut s, _) = business.accept().await.unwrap();
            let mut buf = vec![0u8; 1024];
            let n = s.read(&mut buf).await.unwrap();
            assert!(n > 0);
            s.write_all(b"BUSINESS-REPLY").await.unwrap();
            tokio::time::sleep(std::time::Duration::from_millis(200)).await;
        });

        let front = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let front_addr = front.local_addr().unwrap();
        drop(front);
        let (_tx, rx) = tokio::sync::oneshot::channel::<()>();
        let front_task = tokio::spawn(run_front(front_addr, business_addr, Some(acceptor), async move {
            let _ = rx.await;
        }));
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;

        // TLS client with pinned-test verifier.
        let provider = Arc::new(rustls::crypto::aws_lc_rs::default_provider());
        let client_config = rustls::ClientConfig::builder_with_provider(provider)
            .with_protocol_versions(&[&rustls::version::TLS13])
            .unwrap()
            .dangerous()
            .with_custom_certificate_verifier(Arc::new(AcceptAny))
            .with_no_client_auth();
        let connector = tokio_rustls::TlsConnector::from(Arc::new(client_config));
        let tcp = TcpStream::connect(front_addr).await.unwrap();
        let server_name = rustls::pki_types::ServerName::try_from("localhost").unwrap();
        let mut tls = connector.connect(server_name, tcp).await.unwrap();
        tls.write_all(b"HELLO-FRONT").await.unwrap();
        let mut out = Vec::new();
        tls.read_to_end(&mut out).await.unwrap();
        assert_eq!(out, b"BUSINESS-REPLY");
        front_task.abort();
    }

    #[tokio::test]
    async fn plaintext_front_still_proxies() {
        let business = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let business_addr = business.local_addr().unwrap();
        tokio::spawn(async move {
            let (mut s, _) = business.accept().await.unwrap();
            let mut buf = vec![0u8; 1024];
            let n = s.read(&mut buf).await.unwrap();
            assert!(n > 0);
            s.write_all(b"PLAIN-REPLY").await.unwrap();
            tokio::time::sleep(std::time::Duration::from_millis(200)).await;
        });
        let front = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let front_addr = front.local_addr().unwrap();
        drop(front);
        let (_tx, rx) = tokio::sync::oneshot::channel::<()>();
        let front_task = tokio::spawn(run_front(front_addr, business_addr, None, async move {
            let _ = rx.await;
        }));
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        let mut tcp = TcpStream::connect(front_addr).await.unwrap();
        tcp.write_all(b"HELLO-PLAIN").await.unwrap();
        let mut out = Vec::new();
        tcp.read_to_end(&mut out).await.unwrap();
        assert_eq!(out, b"PLAIN-REPLY");
        front_task.abort();
    }
}
