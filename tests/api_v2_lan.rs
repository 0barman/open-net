use std::net::Ipv4Addr;

#[test]
fn lan_query_exposes_fallible_result_without_a_client() -> Result<(), Box<dyn std::error::Error>> {
    let query: fn() -> Result<Option<Ipv4Addr>, open_net::NetError> =
        open_net::network::preferred_lan_ipv4;
    // OS failure is an allowed environmental outcome; this checks the public
    // signature without depending on the host having a LAN interface.
    let _outcome = query();
    Ok(())
}
