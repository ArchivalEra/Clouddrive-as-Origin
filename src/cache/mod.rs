// `cache::cache` is the module named after the type it owns (`Cache`) and the
// file every ADR and the module map point at; renaming it would churn every
// import path in the tree and in the docs for the sake of a lint.
#[allow(clippy::module_inception)]
pub mod cache;
pub mod flight;
pub(crate) mod ledger;
pub mod leases;
pub(crate) mod magazine;
pub(crate) mod protection;
pub(crate) mod ranged;
pub(crate) mod session;
pub(crate) mod staging;
pub(crate) mod window;
pub mod watch;
pub mod meta;
pub mod persist;
pub mod store;
