use crate::{
    transformer::TransformResultHelper, TransformContext, TransformResult, Transformer,
    TransformerInitContext, Transformers,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;

mod selector;
use selector::Selector;

#[derive(Serialize, Deserialize, Debug, Clone, Eq, PartialEq, Hash)]
#[serde(untagged)]
enum ReplaceInvalid {
    Rule(Box<Transformers>),
    Json(String),
}

impl Default for ReplaceInvalid {
    fn default() -> Self {
        Self::Json("{}".to_string())
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, Eq, PartialEq, Hash)]
enum OnInvalid {
    AsIs,
    ReplaceWith(ReplaceInvalid),
    Error,
}

impl Default for OnInvalid {
    fn default() -> Self {
        Self::ReplaceWith(ReplaceInvalid::default())
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, Eq, PartialEq, Hash)]
struct Field {
    name: String,
    selector: Selector,
    rule: Transformers,
    #[serde(default)]
    quote: bool,
}

#[derive(Serialize, Deserialize, Debug, Clone, Eq, PartialEq, Hash)]
pub struct JsonTransformer {
    fields: Vec<Field>,
    #[serde(default)]
    on_invalid: OnInvalid,
}

impl Transformer for JsonTransformer {
    fn transform(
        &self,
        field_name: &str,
        field_value: &str,
        ctx: &Option<TransformContext>,
    ) -> TransformResult {
        match serde_json::from_str(field_value) {
            Ok(parsed_value) => {
                let mut value = parsed_value;
                let mut err = None;
                for field in &self.fields {
                    let replace_result = field.selector.replace(value, &mut |v| {
                        let transform_result =
                            field
                                .rule
                                .transform(field_name, v.to_string().as_str(), ctx);
                        match transform_result {
                            Ok(r) => match r {
                                Some(v) => {
                                    if field.quote {
                                        return Some(Value::from(v));
                                    }
                                    let tr_json = serde_json::from_str(v.as_str());
                                    match tr_json {
                                        Ok(json) => Some(json),
                                        Err(e) => {
                                            err = Some(TransformResult::error(
                                                field_name,
                                                field_value,
                                                e.to_string().as_str(),
                                            ));
                                            None
                                        }
                                    }
                                }
                                None => None,
                            },
                            Err(e) => {
                                err = Some(Err(e));
                                None
                            }
                        }
                    });

                    if let Some(e) = err {
                        return e;
                    }

                    match replace_result {
                        Ok(v) => value = v,
                        Err(e) => {
                            return TransformResult::error(
                                field_name,
                                field_value,
                                e.to_string().as_str(),
                            );
                        }
                    }
                }
                TransformResult::present(value.to_string())
            }
            Err(e) => {
                // invalid JSON from DB
                match &self.on_invalid {
                    OnInvalid::AsIs => TransformResult::present(field_value.to_string()),
                    OnInvalid::Error => {
                        TransformResult::error(field_name, field_value, e.to_string().as_str())
                    }
                    OnInvalid::ReplaceWith(replacement) => match replacement {
                        ReplaceInvalid::Json(str) => TransformResult::present(str.clone()),
                        ReplaceInvalid::Rule(t) => t.transform(field_name, field_value, ctx),
                    },
                }
            }
        }
    }

    fn init(&mut self, ctx: &TransformerInitContext) {
        for field in &mut self.fields {
            field.rule.init(ctx)
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::Transformers;
    use serde_json::json;

    #[test]
    fn transform() {
        let config = r#"
            json:
              fields:
                - name: "user_name"
                  selector: "$..user.name"
                  quote: true
                  rule:
                    template:
                      format: "UserName"
                - name: "user_age"
                  selector: "$..user.age"
                  rule:
                    random_num:
                      min: 25
                      max: 55
        "#;
        let json = json!(
            [
                { "user": { "name": "Andrew", "age": 40, "comment": "Abc" } },
                { "user": { "name": "Briana", "age": 30, "comment": "Def" } },
                { "user": { "name": "Charlie", "age": 20, "comment": "Ghi" } }
            ]
        );
        let mut t: Transformers = serde_yaml::from_str(config).unwrap();
        t.init(&TransformerInitContext::default());

        let new_json: Value = serde_json::from_str(
            t.transform("field", json.to_string().as_str(), &None)
                .unwrap()
                .unwrap()
                .as_str(),
        )
        .unwrap();
        for i in 0..=2 {
            let new_user = &new_json[i]["user"];
            assert_eq!(new_user["name"], "UserName");
            let age = new_user["age"].as_u64().unwrap();
            assert!(age >= 25 && age <= 55);
            assert_eq!(new_user["comment"], json[i]["user"]["comment"]);
        }
    }

    mod on_invalid {}
}
