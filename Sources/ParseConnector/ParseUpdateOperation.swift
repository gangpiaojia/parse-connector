
public enum ParseUpdateOperation {
    
    case set(BSON)
    
    case increment(BSON)
    
    case multiply(BSON)
    
    case max(BSON)
    
    case min(BSON)
    
    case addToSet(BSON)
    
    case push(BSON)
    
    case popFirst
    
    case popLast
}

extension ParseUpdateOperation {
    
    var value: BSON? {
        guard case let .set(value) = self else { return nil }
        return value
    }
}

extension Dictionary where Key == String, Value == ParseUpdateOperation {
    
    func toBSONDocument() -> BSONDocument {
        
        var set: BSONDocument = [:]
        var unset: BSONDocument = [:]
        var inc: BSONDocument = [:]
        var mul: BSONDocument = [:]
        var max: BSONDocument = [:]
        var min: BSONDocument = [:]
        var addToSet: BSONDocument = [:]
        var push: BSONDocument = [:]
        var pop: BSONDocument = [:]
        
        for (key, value) in self {
            switch value {
            case .set(.null), .set(.undefined): unset[key] = ""
            case let .set(value): set[key] = value
            case let .increment(value): inc[key] = value
            case let .multiply(value): mul[key] = value
            case let .max(value): max[key] = value
            case let .min(value): min[key] = value
            case let .addToSet(value): addToSet[key] = value
            case let .push(value): push[key] = value
            case .popFirst: pop[key] = -1
            case .popLast: pop[key] = 1
            }
        }
        
        var update: BSONDocument = [:]
        if !set.isEmpty { update["$set"] = BSON(set) }
        if !unset.isEmpty { update["$unset"] = BSON(unset) }
        if !inc.isEmpty { update["$inc"] = BSON(inc) }
        if !mul.isEmpty { update["$mul"] = BSON(mul) }
        if !max.isEmpty { update["$max"] = BSON(max) }
        if !min.isEmpty { update["$min"] = BSON(min) }
        if !addToSet.isEmpty { update["$addToSet"] = BSON(push) }
        if !push.isEmpty { update["$push"] = BSON(push) }
        if !pop.isEmpty { update["$pop"] = BSON(pop) }
        return update
    }
}
