open OUnit
open Mqtt

let _ =
    let suite = "mqtt">:::Mqtt.tests in
    run_test_tt_main suite


