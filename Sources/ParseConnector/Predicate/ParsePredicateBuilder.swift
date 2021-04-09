
public struct ParsePredicateBuilder {
    
}

extension ParsePredicateBuilder {
    
    public var id: ParsePredicateValue {
        return self["_id"]
    }
    
    public var createdAt: ParsePredicateValue {
        return self["_created_at"]
    }
    
    public var updatedAt: ParsePredicateValue {
        return self["_updated_at"]
    }
    
    public subscript(_ key: String) -> ParsePredicateValue {
        return .key(key)
    }
}
