#[cfg(feature = "sparql")]
pub mod sparql {
    use anyhow::Result;
    use spargebra::Query;

    /// Basic SPARQL parser for zGraph
    pub struct SparqlParser;

    impl SparqlParser {
        /// Parse a SPARQL query string into a structured Query
        pub fn parse(query: &str) -> Result<Query> {
            let parsed_query = spargebra::Query::parse(query, None)?;
            Ok(parsed_query)
        }
    }
}

#[cfg(test)]
#[cfg(feature = "sparql")]
mod tests {
    use super::*;

    #[test]
    fn test_sparql_parsing() -> Result<()> {
        let query = "SELECT ?s ?p ?o WHERE { ?s ?p ?o . }";
        let parsed = sparql::SparqlParser::parse(query)?;
        println!("Parsed SPARQL query: {:?}", parsed);
        Ok(())
    }

    #[test]
    fn test_sparql_dependencies() {
        // Test that we can use the SPARQL dependencies
        use spargebra::Query;
        use oxrdf::NamedNodeRef;

        let query_str = "SELECT ?s WHERE { ?s <http://example.org/pred> ?o . }";
        let query = Query::parse(query_str, None);
        assert!(query.is_ok());

        // Test that we can create basic RDF components
        let node = NamedNodeRef::new("http://example.org/test").unwrap();
        assert_eq!(node.as_str(), "http://example.org/test");
    }
}