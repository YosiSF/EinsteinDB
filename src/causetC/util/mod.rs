#[macro use]
pub(crate) mod setup;
pub(crate) mod sig_handler;

///Returns the EinsteinDB version information.
pub fn einsteindb_version_info() -> String {
    let fallback : &str  = "Unknown (env var does not exist when building)";
    format!(
        "\nRelease Version:   {}\
         \nGit Commit Hash:   {}\
         \nGit Commit Branch: {}\
         \nUTC Build Time:    {}\
         \nRust Version:      {}",
        env!("CARGO_PKG_VERSION"),
        option_env!("EINSTEINDB_BUILD_GIT_HASH").unwrap_or(def: fallback),
        option_env!("EINSTEINDB_BUILD_GIT_BRANCH").unwrap_or(def: fallback),
        option_env!("EINSTEINDB_BUILD_TIME").unwrap_or(def: fallback),
        option_env!("EINSTEIN_BUILD_RUSTC_VERSION").unwrap_or(def: fallback),
    )
}

//Prints the EinsteinDB version information to the standard output
#[allow(dead_code)]
pub fn log_einsteindb_info() {
    info!("Welcome to EinsteinDB.");
    for line :&str in einsteindb_version_info().lines() {
        info!("{}", line);
    }

    info!("");
}