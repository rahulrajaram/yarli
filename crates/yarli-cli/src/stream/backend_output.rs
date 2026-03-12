use serde_json::Value;

pub fn normalize_output_lines(raw: &str) -> Vec<String> {
    normalize_output_lines_with_options(raw, false)
}

pub fn normalize_output_lines_with_options(raw: &str, verbose: bool) -> Vec<String> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return Vec::new();
    }

    if let Ok(value) = serde_json::from_str::<Value>(trimmed) {
        if let Some(lines) = normalize_codex_event(&value, verbose) {
            return lines;
        }
        if let Some(lines) = normalize_claude_event(&value) {
            return lines;
        }
    }

    raw.lines()
        .map(str::trim_end)
        .filter(|line| !line.trim().is_empty())
        .map(ToString::to_string)
        .collect()
}

fn normalize_codex_event(value: &Value, verbose: bool) -> Option<Vec<String>> {
    let event_type = value.get("type")?.as_str()?;
    match event_type {
        "thread.started" | "turn.started" => Some(Vec::new()),
        "turn.completed" => {
            if verbose {
                Some(summarize_usage(value).into_iter().collect())
            } else {
                Some(Vec::new())
            }
        }
        "item.started" | "item.completed" | "item.failed" => summarize_item_event(value, verbose),
        _ => None,
    }
}

fn normalize_claude_event(value: &Value) -> Option<Vec<String>> {
    let event_type = value.get("type")?.as_str()?;
    if !matches!(event_type, "assistant" | "assistant_message") {
        return None;
    }

    let message = value.get("message").unwrap_or(value);
    summarize_claude_message(message).or_else(|| {
        message
            .get("text")
            .and_then(Value::as_str)
            .map(split_visible_lines)
    })
}

fn summarize_claude_message(message: &Value) -> Option<Vec<String>> {
    message.get("content").and_then(|content| {
        if let Some(text) = content.as_str() {
            Some(split_visible_lines(text))
        } else if let Some(blocks) = content.as_array() {
            let lines: Vec<String> = blocks
                .iter()
                .filter_map(|block| block.get("text").and_then(Value::as_str))
                .flat_map(split_visible_lines)
                .collect();
            if lines.is_empty() {
                None
            } else {
                Some(lines)
            }
        } else {
            None
        }
    })
}

fn summarize_usage(value: &Value) -> Option<String> {
    let usage = value.get("usage")?;
    let input = usage
        .get("input_tokens")
        .and_then(Value::as_u64)
        .unwrap_or(0);
    let cached = usage
        .get("cached_input_tokens")
        .and_then(Value::as_u64)
        .unwrap_or(0);
    let output = usage
        .get("output_tokens")
        .and_then(Value::as_u64)
        .unwrap_or(0);
    Some(format!(
        "usage: input={input} cached={cached} output={output}"
    ))
}

fn summarize_item_event(value: &Value, verbose: bool) -> Option<Vec<String>> {
    let item = value.get("item")?;
    let item_type = item.get("type").and_then(Value::as_str)?;
    match item_type {
        "agent_message" => item
            .get("text")
            .and_then(Value::as_str)
            .map(split_visible_lines),
        "command_execution" => summarize_command_execution(item, verbose),
        "mcp_tool_call" => summarize_mcp_tool_call(item, verbose),
        _ => Some(Vec::new()),
    }
}

fn summarize_command_execution(item: &Value, verbose: bool) -> Option<Vec<String>> {
    if !verbose {
        return Some(Vec::new());
    }

    let command = item
        .get("command")
        .and_then(Value::as_str)
        .map(summarize_command)
        .unwrap_or_else(|| "shell command".to_string());
    let status = item
        .get("status")
        .and_then(Value::as_str)
        .unwrap_or_default();
    let exit_code = item.get("exit_code").and_then(Value::as_i64);

    if status == "in_progress" {
        return Some(vec![format!("shell: {command}")]);
    }

    if let Some(code) = exit_code {
        if code != 0 {
            return Some(vec![format!("shell failed ({code}): {command}")]);
        }
    }

    Some(Vec::new())
}

fn summarize_mcp_tool_call(item: &Value, verbose: bool) -> Option<Vec<String>> {
    if !verbose {
        return Some(Vec::new());
    }

    let server = item.get("server").and_then(Value::as_str).unwrap_or("mcp");
    let tool = item.get("tool").and_then(Value::as_str).unwrap_or("tool");
    let status = item
        .get("status")
        .and_then(Value::as_str)
        .unwrap_or_default();

    if status == "in_progress" {
        return Some(vec![format!("tool: {server}.{tool}")]);
    }

    if item.get("error").is_some_and(|value| !value.is_null()) {
        return Some(vec![format!("tool error: {server}.{tool}")]);
    }

    Some(Vec::new())
}

fn summarize_command(command: &str) -> String {
    command
        .replace("/usr/bin/", "")
        .replace("/bin/", "")
        .trim()
        .to_string()
}

fn split_visible_lines(text: &str) -> Vec<String> {
    text.lines()
        .map(str::trim_end)
        .filter(|line| !line.trim().is_empty())
        .map(ToString::to_string)
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn suppresses_codex_thread_noise() {
        assert!(normalize_output_lines(r#"{"type":"thread.started","thread_id":"x"}"#).is_empty());
        assert!(normalize_output_lines(r#"{"type":"turn.started"}"#).is_empty());
    }

    #[test]
    fn extracts_agent_message_lines() {
        let lines = normalize_output_lines(
            r#"{"type":"item.completed","item":{"type":"agent_message","text":"- one\n- two"}}"#,
        );
        assert_eq!(lines, vec!["- one", "- two"]);
    }

    #[test]
    fn extracts_claude_assistant_lines() {
        let lines = normalize_output_lines(
            r#"{"type":"assistant","message":{"content":[{"type":"text","text":"- one"},{"type":"text","text":"- two"}]}}"#,
        );

        assert_eq!(lines, vec!["- one", "- two"]);
    }

    #[test]
    fn extracts_claude_assistant_raw_message_text() {
        let lines = normalize_output_lines(
            r#"{"type":"assistant","message":{"text":"Hello\nstream output"}}"#,
        );

        assert_eq!(lines, vec!["Hello", "stream output"]);
    }

    #[test]
    fn hides_tooling_noise_in_default_mode() {
        let command_lines = normalize_output_lines(
            r#"{"type":"item.started","item":{"type":"command_execution","command":"/usr/bin/zsh -lc \"sed -n '1,10p' README.md\"","status":"in_progress"}}"#,
        );
        let mcp_lines = normalize_output_lines(
            r#"{"type":"item.started","item":{"type":"mcp_tool_call","server":"haake-memory","tool":"query_memories","status":"in_progress"}}"#,
        );
        let usage_lines = normalize_output_lines(
            r#"{"type":"turn.completed","usage":{"input_tokens":10,"cached_input_tokens":2,"output_tokens":7}}"#,
        );

        assert!(command_lines.is_empty());
        assert!(mcp_lines.is_empty());
        assert!(usage_lines.is_empty());
    }

    #[test]
    fn summarizes_command_execution_start_in_verbose_mode() {
        let lines = normalize_output_lines_with_options(
            r#"{"type":"item.started","item":{"type":"command_execution","command":"/usr/bin/zsh -lc \"sed -n '1,10p' README.md\"","status":"in_progress"}}"#,
            true,
        );
        assert_eq!(lines, vec![r#"shell: zsh -lc "sed -n '1,10p' README.md""#]);
    }

    #[test]
    fn summarizes_mcp_tool_start_in_verbose_mode() {
        let lines = normalize_output_lines_with_options(
            r#"{"type":"item.started","item":{"type":"mcp_tool_call","server":"haake-memory","tool":"query_memories","status":"in_progress"}}"#,
            true,
        );
        assert_eq!(lines, vec!["tool: haake-memory.query_memories"]);
    }

    #[test]
    fn summarizes_turn_usage_in_verbose_mode() {
        let lines = normalize_output_lines_with_options(
            r#"{"type":"turn.completed","usage":{"input_tokens":10,"cached_input_tokens":2,"output_tokens":7}}"#,
            true,
        );
        assert_eq!(lines, vec!["usage: input=10 cached=2 output=7"]);
    }

    #[test]
    fn passes_plain_text_through() {
        let lines = normalize_output_lines("line one\nline two\n");
        assert_eq!(lines, vec!["line one", "line two"]);
    }
}
