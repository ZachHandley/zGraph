//! Template engine for rendering notification content

use std::collections::HashMap;

use anyhow::Result;
use handlebars::{Handlebars, Helper, Context, RenderContext, Output, HelperResult};
use rust_embed::RustEmbed;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::models::{NotificationTemplate, NotificationType};
use crate::NotificationError;

/// Embedded template assets
#[derive(RustEmbed)]
#[folder = "templates/"]
struct TemplateAssets;

/// Template engine for rendering notification content
pub struct TemplateEngine {
    handlebars: Handlebars<'static>,
}

impl TemplateEngine {
    /// Create a new template engine with built-in helpers
    pub fn new() -> Result<Self> {
        let mut handlebars = Handlebars::new();

        // Register built-in helpers
        handlebars.register_helper("format_date", Box::new(format_date_helper));
        handlebars.register_helper("format_currency", Box::new(format_currency_helper));
        handlebars.register_helper("truncate", Box::new(truncate_helper));
        handlebars.register_helper("upper", Box::new(upper_helper));
        handlebars.register_helper("lower", Box::new(lower_helper));
        handlebars.register_helper("pluralize", Box::new(pluralize_helper));

        // Load built-in templates
        for file in TemplateAssets::iter() {
            if let Some(content) = TemplateAssets::get(&file) {
                let template_content = std::str::from_utf8(&content.data)?;
                handlebars.register_template_string(&file, template_content)?;
            }
        }

        Ok(Self { handlebars })
    }

    /// Render a template with the given context
    pub fn render(
        &self,
        template: &NotificationTemplate,
        context: &TemplateContext,
    ) -> Result<RenderedTemplate> {
        // Validate template variables
        self.validate_template_context(template, context)?;

        // Render subject
        let subject = self.handlebars.render_template(
            &template.subject_template,
            &context.variables,
        )?;

        // Render HTML body if available
        let html_body = if let Some(html_template) = &template.html_body_template {
            self.handlebars.render_template(html_template, &context.variables)?
        } else {
            // For SMS or plain text emails, convert subject to HTML
            if template.notification_type == NotificationType::Sms {
                subject.clone()
            } else {
                format!("<html><body><p>{}</p></body></html>", html_escape(&subject))
            }
        };

        // Render text body if available
        let text_body = if let Some(text_template) = &template.text_body_template {
            Some(self.handlebars.render_template(text_template, &context.variables)?)
        } else if template.notification_type == NotificationType::Sms {
            // For SMS, use subject as text body
            Some(subject.clone())
        } else {
            // Auto-generate text from HTML
            Some(html_to_text(&html_body))
        };

        Ok(RenderedTemplate {
            subject,
            html_body,
            text_body,
        })
    }

    /// Validate that all required template variables are provided
    fn validate_template_context(
        &self,
        template: &NotificationTemplate,
        context: &TemplateContext,
    ) -> Result<()> {
        for variable in &template.variables {
            if variable.required && !context.variables.contains_key(&variable.name) {
                return Err(NotificationError::InvalidTemplate(
                    format!("Missing required variable: {}", variable.name)
                ).into());
            }
        }
        Ok(())
    }

    /// Register a custom template
    pub fn register_template(&mut self, name: &str, template: &str) -> Result<()> {
        self.handlebars.register_template_string(name, template)?;
        Ok(())
    }

    /// Get available built-in templates
    pub fn list_builtin_templates() -> Vec<String> {
        TemplateAssets::iter().map(|s| s.to_string()).collect()
    }
}

/// Context for template rendering
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TemplateContext {
    pub variables: HashMap<String, Value>,
}

impl TemplateContext {
    /// Create a new template context
    pub fn new() -> Self {
        Self {
            variables: HashMap::new(),
        }
    }

    /// Add a variable to the context
    pub fn insert<T: Serialize>(mut self, key: &str, value: &T) -> Result<Self> {
        self.variables.insert(key.to_string(), serde_json::to_value(value)?);
        Ok(self)
    }

    /// Add a string variable to the context
    pub fn insert_str(mut self, key: &str, value: &str) -> Self {
        self.variables.insert(key.to_string(), Value::String(value.to_string()));
        self
    }

    /// Add a number variable to the context
    pub fn insert_number(mut self, key: &str, value: f64) -> Self {
        self.variables.insert(key.to_string(), Value::Number(serde_json::Number::from_f64(value).unwrap()));
        self
    }

    /// Add a boolean variable to the context
    pub fn insert_bool(mut self, key: &str, value: bool) -> Self {
        self.variables.insert(key.to_string(), Value::Bool(value));
        self
    }

    /// Create context from a JSON value
    pub fn from_json(value: Value) -> Result<Self> {
        let variables = match value {
            Value::Object(map) => map.into_iter().collect(),
            _ => return Err(NotificationError::InvalidTemplate(
                "Context must be a JSON object".to_string()
            ).into()),
        };

        Ok(Self { variables })
    }
}

impl Default for TemplateContext {
    fn default() -> Self {
        Self::new()
    }
}

/// Rendered template content
#[derive(Debug, Clone)]
pub struct RenderedTemplate {
    pub subject: String,
    pub html_body: String,
    pub text_body: Option<String>,
}

// Handlebars helpers

fn format_date_helper(
    h: &Helper,
    _: &Handlebars,
    _: &Context,
    _: &mut RenderContext,
    out: &mut dyn Output,
) -> HelperResult {
    let timestamp = h.param(0)
        .and_then(|v| v.value().as_i64())
        .ok_or_else(|| handlebars::RenderError::new("Invalid timestamp"))?;

    let format = h.param(1)
        .and_then(|v| v.value().as_str())
        .unwrap_or("%Y-%m-%d %H:%M:%S");

    let dt = chrono::DateTime::from_timestamp(timestamp, 0)
        .ok_or_else(|| handlebars::RenderError::new("Invalid timestamp"))?;

    out.write(&dt.format(format).to_string())?;
    Ok(())
}

fn format_currency_helper(
    h: &Helper,
    _: &Handlebars,
    _: &Context,
    _: &mut RenderContext,
    out: &mut dyn Output,
) -> HelperResult {
    let amount = h.param(0)
        .and_then(|v| v.value().as_f64())
        .ok_or_else(|| handlebars::RenderError::new("Invalid amount"))?;

    let currency = h.param(1)
        .and_then(|v| v.value().as_str())
        .unwrap_or("USD");

    let formatted = match currency {
        "USD" => format!("${:.2}", amount),
        "EUR" => format!("€{:.2}", amount),
        "GBP" => format!("£{:.2}", amount),
        _ => format!("{} {:.2}", currency, amount),
    };

    out.write(&formatted)?;
    Ok(())
}

fn truncate_helper(
    h: &Helper,
    _: &Handlebars,
    _: &Context,
    _: &mut RenderContext,
    out: &mut dyn Output,
) -> HelperResult {
    let text = h.param(0)
        .and_then(|v| v.value().as_str())
        .ok_or_else(|| handlebars::RenderError::new("Invalid text"))?;

    let length = h.param(1)
        .and_then(|v| v.value().as_u64())
        .unwrap_or(100) as usize;

    let truncated = if text.len() > length {
        format!("{}...", &text[..length])
    } else {
        text.to_string()
    };

    out.write(&truncated)?;
    Ok(())
}

fn upper_helper(
    h: &Helper,
    _: &Handlebars,
    _: &Context,
    _: &mut RenderContext,
    out: &mut dyn Output,
) -> HelperResult {
    let text = h.param(0)
        .and_then(|v| v.value().as_str())
        .ok_or_else(|| handlebars::RenderError::new("Invalid text"))?;

    out.write(&text.to_uppercase())?;
    Ok(())
}

fn lower_helper(
    h: &Helper,
    _: &Handlebars,
    _: &Context,
    _: &mut RenderContext,
    out: &mut dyn Output,
) -> HelperResult {
    let text = h.param(0)
        .and_then(|v| v.value().as_str())
        .ok_or_else(|| handlebars::RenderError::new("Invalid text"))?;

    out.write(&text.to_lowercase())?;
    Ok(())
}

fn pluralize_helper(
    h: &Helper,
    _: &Handlebars,
    _: &Context,
    _: &mut RenderContext,
    out: &mut dyn Output,
) -> HelperResult {
    let count = h.param(0)
        .and_then(|v| v.value().as_u64())
        .ok_or_else(|| handlebars::RenderError::new("Invalid count"))?;

    let singular = h.param(1)
        .and_then(|v| v.value().as_str())
        .ok_or_else(|| handlebars::RenderError::new("Missing singular form"))?;

    let default_plural = format!("{}s", singular);
    let plural = h.param(2)
        .and_then(|v| v.value().as_str())
        .unwrap_or(&default_plural);

    let result = if count == 1 { singular } else { plural };
    out.write(result)?;
    Ok(())
}

/// Basic HTML to text conversion
fn html_to_text(html: &str) -> String {
    // This is a simple implementation. For production use, consider using a proper HTML parser
    html.replace("<br>", "\n")
        .replace("<br/>", "\n")
        .replace("<br />", "\n")
        .replace("</p>", "\n\n")
        .replace("</div>", "\n")
        .replace("</h1>", "\n\n")
        .replace("</h2>", "\n\n")
        .replace("</h3>", "\n\n")
        // Remove all other HTML tags
        .chars()
        .collect::<Vec<_>>()
        .split(|&c| c == '<')
        .map(|chars| {
            if let Some(pos) = chars.iter().position(|&c| c == '>') {
                chars[pos + 1..].iter().collect()
            } else {
                chars.iter().collect()
            }
        })
        .collect::<Vec<String>>()
        .join("")
        .trim()
        .to_string()
}

/// Basic HTML escaping
fn html_escape(text: &str) -> String {
    text.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&#x27;")
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_template_context() {
        let context = TemplateContext::new()
            .insert_str("name", "John Doe")
            .insert_number("amount", 100.50)
            .insert_bool("verified", true);

        assert_eq!(context.variables.get("name"), Some(&Value::String("John Doe".to_string())));
        assert_eq!(context.variables.get("verified"), Some(&Value::Bool(true)));
    }

    #[test]
    fn test_html_to_text() {
        let html = "<h1>Hello</h1><p>This is a <strong>test</strong>.</p><br/>New line.";
        let text = html_to_text(html);
        assert!(text.contains("Hello"));
        assert!(text.contains("This is a test."));
        assert!(text.contains("New line."));
    }

    #[test]
    fn test_html_escape() {
        let text = "Hello <world> & \"friends\"";
        let escaped = html_escape(text);
        assert_eq!(escaped, "Hello &lt;world&gt; &amp; &quot;friends&quot;");
    }
}