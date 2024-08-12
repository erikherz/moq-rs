use url::Url;
use crate::{ApiError, Origin};

#[derive(Clone)]
pub struct Client {
    // The address of the moq-api server
    url: Url,

    client: reqwest::Client,
}

impl Client {
    pub fn new(url: Url) -> Self {
        let client = reqwest::Client::new();
        Self { url, client }
    }

    pub async fn get_origin(&self, namespace: &str) -> Result<Option<Origin>, ApiError> {
        // Step 1: Create a mutable local copy of the URL
        let mut working_url = self.url.clone();
        let original_path = working_url.path().to_string();
        let do_edge = original_path.contains("/do_edge/");
        let do_regex = original_path.contains("/do_regex/");
        let mut edge_value = String::new();
        let mut regex_values = (String::new(), String::new(), String::new());

        println!("Original path: {}", original_path);

        if do_edge {
            // Extract value after /do_edge/
            if let Some(edge_pos) = original_path.find("/do_edge/") {
                edge_value = original_path[edge_pos + 9..].to_string();
                println!("Captured do_edge value: {}", edge_value);
            }
            // Strip the /do_edge/ part from the path for the URL construction
            let stripped_path = original_path.replace("/do_edge/", "");
            working_url.set_path(&stripped_path);
        } else if do_regex {
            // Extract values for regex replacement and strip the /do_regex/ part
            if let Some(regex_pos) = original_path.find("/do_regex/") {
                let regex_parts: Vec<&str> = original_path[regex_pos + 10..].split('/').collect();
                if regex_parts.len() == 3 {
                    regex_values = (
                        regex_parts[0].to_string(),  // "ohio"
                        regex_parts[1].to_string(),  // "origin"
                        regex_parts[2].to_string(),  // "regional"
                    );
                    println!("Captured do_regex values: {:?}", regex_values);
                }
                // Strip the /do_regex/ part from the path for the URL construction
                let stripped_path = original_path.replace(&format!("/do_regex/{}/{}/{}", regex_values.0, regex_values.1, regex_values.2), "");
                working_url.set_path(&stripped_path);
            }
        }

        // Step 2: Construct the URL as usual using the local mutable copy
        let url = working_url.join("origin/")?.join(namespace)?;
        println!("Constructed URL: {}", url);
        let resp = self.client.get(url).send().await?;
        if resp.status() == reqwest::StatusCode::NOT_FOUND {
            return Ok(None);
        }

        // Step 3: Apply the do_edge or do_regex logic to the JSON response
        let mut origin: Origin = resp.json().await?;
        println!("Original URL in response: {}", origin.url);

        if do_edge {
            origin.url = Url::parse(&format!("https://{}", edge_value))?;
            println!("Modified URL with do_edge: {}", origin.url);
        } else if do_regex {
            // Only modify the URL if the 'contains' value is not found
            if !origin.url.as_str().contains(&regex_values.0) {
                if origin.url.as_str().contains(&regex_values.1) {
                    let new_url = origin.url.as_str().replace(&regex_values.1, &regex_values.2);
                    origin.url = Url::parse(&new_url)?;
                    println!("Modified URL with do_regex: {}", origin.url);
                } else {
                    println!("No replacement needed, URL remains: {}", origin.url);
                }
            } else {
                println!("'Contains' value found in URL, no modification applied: {}", origin.url);
            }
        }

        Ok(Some(origin))
    }

    pub async fn set_origin(&self, namespace: &str, origin: Origin) -> Result<(), ApiError> {
        let url = self.url.join("origin/")?.join(namespace)?;

        let resp = self.client.post(url).json(&origin).send().await?;
        resp.error_for_status()?;

        Ok(())
    }

    pub async fn delete_origin(&self, namespace: &str) -> Result<(), ApiError> {
        let url = self.url.join("origin/")?.join(namespace)?;

        let resp = self.client.delete(url).send().await?;
        resp.error_for_status()?;

        Ok(())
    }

    pub async fn patch_origin(&self, namespace: &str, origin: Origin) -> Result<(), ApiError> {
        let url = self.url.join("origin/")?.join(namespace)?;

        let resp = self.client.patch(url).json(&origin).send().await?;
        resp.error_for_status()?;

        Ok(())
    }
}
