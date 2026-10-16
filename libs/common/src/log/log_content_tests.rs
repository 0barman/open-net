use super::create_log_content;
use serde_json::json;

#[test]
fn preserves_first_field_order_and_last_duplicate_value() {
    assert_eq!(
        create_log_content(
            Some("z|a|z|m|a"),
            Some(vec![json!(1), json!(2), json!(3), json!(4), json!(5)]),
        ),
        r#"{"z":3,"a":5,"m":4}"#
    );
}

#[test]
fn ignores_unpaired_fields_and_values() {
    assert_eq!(
        create_log_content(Some("value|unused"), Some(vec![json!(1)])),
        r#"{"value":1}"#
    );
    assert_eq!(
        create_log_content(Some("value"), Some(vec![json!(1), json!(2)])),
        r#"{"value":1}"#
    );
}

#[test]
fn missing_fields_or_values_produce_an_empty_object() {
    assert_eq!(create_log_content(None, None), "{}");
    assert_eq!(create_log_content(None, Some(vec![json!(1)])), "{}");
    assert_eq!(create_log_content(Some("value"), None), "{}");
    assert_eq!(create_log_content(Some("value"), Some(vec![])), "{}");
}

#[test]
fn empty_field_names_are_retained_and_deduplicated() {
    assert_eq!(
        create_log_content(Some(""), Some(vec![json!(null)])),
        r#"{"":null}"#
    );
    assert_eq!(
        create_log_content(Some("|a|"), Some(vec![json!(1), json!(2), json!(3)])),
        r#"{"":3,"a":2}"#
    );
}

#[test]
fn serializes_unicode_and_escapes_json_field_names_and_values() {
    assert_eq!(
        create_log_content(
            Some("路径\"\\\n|说明"),
            Some(vec![
                json!(["值", null, true, 1.5]),
                json!("\"\\\n\t\u{0000}"),
            ]),
        ),
        r#"{"路径\"\\\n":["值",null,true,1.5],"说明":"\"\\\n\t\u0000"}"#
    );
}
