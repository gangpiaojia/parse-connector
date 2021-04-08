
public struct ParsePredicateBuilder {
    
}

extension ParsePredicateBuilder {
    
    public subscript(_ key: String) -> ParsePredicateValue {
        return .key(key)
    }
}
