
public indirect enum ParsePredicateExpression {
    
    case not(ParsePredicateExpression)
    
    case equal(ParsePredicateValue, ParsePredicateValue)
    
    case notEqual(ParsePredicateValue, ParsePredicateValue)
    
    case lessThan(ParsePredicateValue, ParsePredicateValue)
    
    case greaterThan(ParsePredicateValue, ParsePredicateValue)
    
    case lessThanOrEqualTo(ParsePredicateValue, ParsePredicateValue)
    
    case greaterThanOrEqualTo(ParsePredicateValue, ParsePredicateValue)
    
    case containsIn(ParsePredicateValue, ParsePredicateValue)
    
    case notContainsIn(ParsePredicateValue, ParsePredicateValue)
    
    case matching(ParsePredicateValue, Regex)
    
    case and(ParsePredicateExpression, ParsePredicateExpression)
    
    case or(ParsePredicateExpression, ParsePredicateExpression)
}

public enum ParsePredicateValue {
    
    case key(String)
    
    case value(BSONConvertible)
    
    case object(ParseObject)
}

extension ParsePredicateExpression {
    
    var _andList: [ParsePredicateExpression]? {
        switch self {
        case let .and(lhs, rhs):
            let _lhs = lhs._andList ?? [lhs]
            let _rhs = rhs._andList ?? [rhs]
            return _lhs + _rhs
        default: return nil
        }
    }
    
    var _orList: [ParsePredicateExpression]? {
        switch self {
        case let .or(lhs, rhs):
            let _lhs = lhs._orList ?? [lhs]
            let _rhs = rhs._orList ?? [rhs]
            return _lhs + _rhs
        default: return nil
        }
    }
    
    func toBSONDocument() throws -> BSONDocument {
        
        switch self {
        case let .not(x):
            
            return try ["$not": BSON(x.toBSONDocument())]
            
        case let .equal(.key(key), .value(value)),
             let .equal(.value(value), .key(key)):
            
            return try [key: ["$eq": value.toBSON()]]
            
        case let .notEqual(.key(key), .value(value)),
             let .notEqual(.value(value), .key(key)):
            
            return try [key: ["$ne": value.toBSON()]]
            
        case let .equal(.key(key), .object(value)),
             let .equal(.object(value), .key(key)):
            
            guard let objectId = value.id else { throw ParseError.nullObjectId }
            return try [key: ["$eq": "\(value.class)$\(objectId)".toBSON()]]
            
        case let .notEqual(.key(key), .object(value)),
             let .notEqual(.object(value), .key(key)):
            
            guard let objectId = value.id else { throw ParseError.nullObjectId }
            return try [key: ["$ne": "\(value.class)$\(objectId)".toBSON()]]
            
        case let .lessThan(.key(key), .value(value)),
             let .lessThan(.value(value), .key(key)):
            
            return try [key: ["$lt": value.toBSON()]]
            
        case let .greaterThan(.key(key), .value(value)),
             let .greaterThan(.value(value), .key(key)):
            
            return try [key: ["$gt": value.toBSON()]]
            
        case let .lessThanOrEqualTo(.key(key), .value(value)),
             let .lessThanOrEqualTo(.value(value), .key(key)):
            
            return try [key: ["$lte": value.toBSON()]]
            
        case let .greaterThanOrEqualTo(.key(key), .value(value)),
             let .greaterThanOrEqualTo(.value(value), .key(key)):
            
            return try [key: ["$gte": value.toBSON()]]
            
        case let .containsIn(.key(key), .value(value)):
            
            return try [key: ["$in": value.toBSON()]]
            
        case let .notContainsIn(.key(key), .value(value)):
            
            return try [key: ["$nin": value.toBSON()]]
            
        case let .matching(.key(key), regex):
        
            return try [key: ["$regex": regex.toBSON()]]
            
        case let .and(lhs, rhs):
            
            let _lhs = lhs._andList ?? [lhs]
            let _rhs = rhs._andList ?? [rhs]
            let list = _lhs + _rhs
            return try ["$and": BSON(list.map { try $0.toBSONDocument() })]
            
        case let .or(lhs, rhs):
            
            let _lhs = lhs._orList ?? [lhs]
            let _rhs = rhs._orList ?? [rhs]
            let list = _lhs + _rhs
            return try ["$or": BSON(list.map { try $0.toBSONDocument() })]
            
        default: throw ParseError.invalidExpression
        }
    }
}

public func == (lhs: ParsePredicateValue, rhs: _OptionalNilComparisonType) -> ParsePredicateExpression {
    return .equal(lhs, .value(BSON.null))
}

public func != (lhs: ParsePredicateValue, rhs: _OptionalNilComparisonType) -> ParsePredicateExpression {
    return .notEqual(lhs, .value(BSON.null))
}

public func == (lhs: ParsePredicateValue, rhs: ParseObject) -> ParsePredicateExpression {
    return .equal(lhs, .object(rhs))
}

public func != (lhs: ParsePredicateValue, rhs: ParseObject) -> ParsePredicateExpression {
    return .notEqual(lhs, .object(rhs))
}

public func == <T: BSONConvertible>(lhs: ParsePredicateValue, rhs: T) -> ParsePredicateExpression {
    return .equal(lhs, .value(rhs))
}

public func != <T: BSONConvertible>(lhs: ParsePredicateValue, rhs: T) -> ParsePredicateExpression {
    return .notEqual(lhs, .value(rhs))
}

public func < <T: BSONConvertible>(lhs: ParsePredicateValue, rhs: T) -> ParsePredicateExpression {
    return .lessThan(lhs, .value(rhs))
}

public func > <T: BSONConvertible>(lhs: ParsePredicateValue, rhs: T) -> ParsePredicateExpression {
    return .greaterThan(lhs, .value(rhs))
}

public func <= <T: BSONConvertible>(lhs: ParsePredicateValue, rhs: T) -> ParsePredicateExpression {
    return .lessThanOrEqualTo(lhs, .value(rhs))
}

public func >= <T: BSONConvertible>(lhs: ParsePredicateValue, rhs: T) -> ParsePredicateExpression {
    return .greaterThanOrEqualTo(lhs, .value(rhs))
}

public func == (lhs: _OptionalNilComparisonType, rhs: ParsePredicateValue) -> ParsePredicateExpression {
    return .equal(.value(BSON.null), rhs)
}

public func != (lhs: _OptionalNilComparisonType, rhs: ParsePredicateValue) -> ParsePredicateExpression {
    return .notEqual(.value(BSON.null), rhs)
}

public func == (lhs: ParseObject, rhs: ParsePredicateValue) -> ParsePredicateExpression {
    return .equal(.object(lhs), rhs)
}

public func != (lhs: ParseObject, rhs: ParsePredicateValue) -> ParsePredicateExpression {
    return .notEqual(.object(lhs), rhs)
}

public func == <T: BSONConvertible>(lhs: T, rhs: ParsePredicateValue) -> ParsePredicateExpression {
    return .equal(.value(lhs), rhs)
}

public func != <T: BSONConvertible>(lhs: T, rhs: ParsePredicateValue) -> ParsePredicateExpression {
    return .notEqual(.value(lhs), rhs)
}

public func < <T: BSONConvertible>(lhs: T, rhs: ParsePredicateValue) -> ParsePredicateExpression {
    return .lessThan(.value(lhs), rhs)
}

public func > <T: BSONConvertible>(lhs: T, rhs: ParsePredicateValue) -> ParsePredicateExpression {
    return .greaterThan(.value(lhs), rhs)
}

public func <= <T: BSONConvertible>(lhs: T, rhs: ParsePredicateValue) -> ParsePredicateExpression {
    return .lessThanOrEqualTo(.value(lhs), rhs)
}

public func >= <T: BSONConvertible>(lhs: T, rhs: ParsePredicateValue) -> ParsePredicateExpression {
    return .greaterThanOrEqualTo(.value(lhs), rhs)
}

public func ~= (lhs: NSRegularExpression, rhs: ParsePredicateValue) -> ParsePredicateExpression {
    return .matching(rhs, Regex(lhs))
}

public func ~= (lhs: Regex, rhs: ParsePredicateValue) -> ParsePredicateExpression {
    return .matching(rhs, lhs)
}

public func ~= <C: Collection>(lhs: C, rhs: ParsePredicateValue) -> ParsePredicateExpression where C.Element: BSONConvertible {
    return .containsIn(.value(Array(lhs)), rhs)
}

public func ~= <T: BSONConvertible>(lhs: Range<T>, rhs: ParsePredicateValue) -> ParsePredicateExpression {
    return rhs <= lhs.lowerBound && lhs.upperBound < rhs
}

public func ~= <T: BSONConvertible>(lhs: ClosedRange<T>, rhs: ParsePredicateValue) -> ParsePredicateExpression {
    return rhs <= lhs.lowerBound && lhs.upperBound <= rhs
}

public func ~= <T: BSONConvertible>(lhs: PartialRangeFrom<T>, rhs: ParsePredicateValue) -> ParsePredicateExpression {
    return rhs <= lhs.lowerBound
}

public func ~= <T: BSONConvertible>(lhs: PartialRangeUpTo<T>, rhs: ParsePredicateValue) -> ParsePredicateExpression {
    return lhs.upperBound < rhs
}

public func ~= <T: BSONConvertible>(lhs: PartialRangeThrough<T>, rhs: ParsePredicateValue) -> ParsePredicateExpression {
    return lhs.upperBound <= rhs
}

public func =~ (lhs: ParsePredicateValue, rhs: NSRegularExpression) -> ParsePredicateExpression {
    return .matching(lhs, Regex(rhs))
}

public func =~ (lhs: ParsePredicateValue, rhs: Regex) -> ParsePredicateExpression {
    return .matching(lhs, rhs)
}

public func =~ <C: Collection>(lhs: ParsePredicateValue, rhs: C) -> ParsePredicateExpression where C.Element: BSONConvertible {
    return .containsIn(lhs, .value(Array(rhs)))
}

public func =~ <T: BSONConvertible>(lhs: ParsePredicateValue, rhs: Range<T>) -> ParsePredicateExpression {
    return lhs <= rhs.lowerBound && rhs.upperBound < lhs
}

public func =~ <T: BSONConvertible>(lhs: ParsePredicateValue, rhs: ClosedRange<T>) -> ParsePredicateExpression {
    return lhs <= rhs.lowerBound && rhs.upperBound <= lhs
}

public func =~ <T: BSONConvertible>(lhs: ParsePredicateValue, rhs: PartialRangeFrom<T>) -> ParsePredicateExpression {
    return lhs <= rhs.lowerBound
}

public func =~ <T: BSONConvertible>(lhs: ParsePredicateValue, rhs: PartialRangeUpTo<T>) -> ParsePredicateExpression {
    return rhs.upperBound < lhs
}

public func =~ <T: BSONConvertible>(lhs: ParsePredicateValue, rhs: PartialRangeThrough<T>) -> ParsePredicateExpression {
    return rhs.upperBound <= lhs
}

public prefix func !(x: ParsePredicateExpression) -> ParsePredicateExpression {
    return .not(x)
}

public func && (lhs: ParsePredicateExpression, rhs: ParsePredicateExpression) -> ParsePredicateExpression {
    return .and(lhs, rhs)
}

public func || (lhs: ParsePredicateExpression, rhs: ParsePredicateExpression) -> ParsePredicateExpression {
    return .or(lhs, rhs)
}
