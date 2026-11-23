use anyhow::{Context, Result};
use regex::Regex;
use serde::{Deserialize, Serialize};
use std::collections::HashSet;

/// Set of exclusions to apply before export.
#[derive(Debug, Clone, Default, Serialize)]
pub struct ExclusionSet {
    /// Workspaces to exclude (full paths).
    pub excluded_workspaces: HashSet<String>,
    /// Conversation IDs to exclude.
    pub excluded_conversations: HashSet<i64>,
    /// Patterns to match against titles for exclusion.
    #[serde(skip)]
    pub excluded_patterns: Vec<Regex>,
    /// Raw pattern strings (for serialization).
    pub excluded_pattern_strings: Vec<String>,
}

#[derive(Deserialize)]
struct SerializedExclusionSet {
    #[serde(default)]
    excluded_workspaces: HashSet<String>,
    #[serde(default)]
    excluded_conversations: HashSet<i64>,
    #[serde(default)]
    excluded_pattern_strings: Vec<String>,
}

impl<'de> Deserialize<'de> for ExclusionSet {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let serialized = SerializedExclusionSet::deserialize(deserializer)?;
        let mut exclusions = Self {
            excluded_workspaces: serialized.excluded_workspaces,
            excluded_conversations: serialized.excluded_conversations,
            excluded_patterns: Vec::new(),
            excluded_pattern_strings: serialized.excluded_pattern_strings,
        };
        exclusions
            .compile_patterns()
            .map_err(serde::de::Error::custom)?;
        Ok(exclusions)
    }
}

impl ExclusionSet {
    /// Create a new empty exclusion set.
    pub fn new() -> Self {
        Self::default()
    }

    /// Add a workspace to exclusions.
    pub fn exclude_workspace(&mut self, workspace: &str) {
        self.excluded_workspaces.insert(workspace.to_string());
    }

    /// Remove a workspace from exclusions.
    pub fn include_workspace(&mut self, workspace: &str) {
        self.excluded_workspaces.remove(workspace);
    }

    /// Add a conversation to exclusions.
    pub fn exclude_conversation(&mut self, conversation_id: i64) {
        self.excluded_conversations.insert(conversation_id);
    }

    /// Remove a conversation from exclusions.
    pub fn include_conversation(&mut self, conversation_id: i64) {
        self.excluded_conversations.remove(&conversation_id);
    }

    /// Check if a workspace is excluded.
    pub fn is_workspace_excluded(&self, workspace: &str) -> bool {
        self.excluded_workspaces.contains(workspace)
    }

    /// Check if a conversation is excluded.
    pub fn is_conversation_excluded(&self, conversation_id: i64) -> bool {
        self.excluded_conversations.contains(&conversation_id)
    }

    /// Check if a title matches any exclusion pattern.
    pub fn is_excluded(&self, title: &str) -> bool {
        self.excluded_patterns.iter().any(|re| re.is_match(title))
    }

    /// Add a title pattern to exclusions.
    pub fn add_pattern(&mut self, pattern: &str) -> Result<()> {
        let regex = Regex::new(pattern).context("Invalid exclusion pattern")?;
        self.excluded_patterns.push(regex);
        self.excluded_pattern_strings.push(pattern.to_string());
        Ok(())
    }

    /// Check if an item should be excluded based on all criteria.
    pub fn should_exclude(
        &self,
        workspace: Option<&str>,
        conversation_id: i64,
        title: &str,
    ) -> bool {
        if let Some(ws) = workspace
            && self.is_workspace_excluded(ws)
        {
            return true;
        }
        if self.is_conversation_excluded(conversation_id) {
            return true;
        }
        self.is_excluded(title)
    }

    /// Get the count of excluded items.
    pub fn exclusion_counts(&self) -> (usize, usize, usize) {
        (
            self.excluded_workspaces.len(),
            self.excluded_conversations.len(),
            self.excluded_patterns.len(),
        )
    }

    /// Check if any exclusions are active.
    pub fn has_exclusions(&self) -> bool {
        !self.excluded_workspaces.is_empty()
            || !self.excluded_conversations.is_empty()
            || !self.excluded_patterns.is_empty()
    }

    /// Re-compile patterns from strings (for deserialization).
    pub fn compile_patterns(&mut self) -> Result<()> {
        let compiled = self
            .excluded_pattern_strings
            .iter()
            .map(|pattern_str| {
                Regex::new(pattern_str)
                    .with_context(|| format!("Invalid exclusion pattern: {pattern_str}"))
            })
            .collect::<Result<Vec<_>>>()?;
        self.excluded_patterns = compiled;
        Ok(())
    }
}
