
import SwiftBSON

public struct ParseObject {
    
    let `class`: String
    
    var data: BSONDocument
    
    var updated: BSONDocument = [:]
    
    init(class: String, data: BSONDocument) {
        self.class = `class`
        self.data = data
    }
    
    public init(_ class: String, id: String? = nil) {
        self.class = `class`
        self.data = [:]
        if let id = id {
            self.data["_id"] = (try? .objectID(BSONObjectID(id))) ?? .string(id)
        }
    }
}

extension ParseObject {
    
    public var id: String? {
        let id = data["_id"]
        return id?.objectIDValue?.hex ?? id?.stringValue
    }
    
    public var createdAt: Date? {
        return self["_created_at"].dateValue
    }
    
    public var updatedAt: Date? {
        return self["_updated_at"].dateValue
    }
    
    public var keys: [String] {
        return Set(data.keys + updated.keys).sorted()
    }
    
    public subscript(_ key: String) -> BSON {
        get {
            return updated[key] ?? data[key] ?? .null
        }
        set {
            guard key != "_id" else { fatalError("_id is not writable") }
            updated[key] = newValue
        }
    }
}

extension ParseObject {
    
    public func fetch(from connection: DBConnection) -> EventLoopFuture<ParseObject?> {
        guard let id = data["_id"] else { return connection.eventLoop.makeFailedFuture(ParseError.nullObjectId) }
        return connection.mongoQuery().collection(`class`).findOne().filter(["_id": id]).execute().map { $0.map { ParseObject(class: `class`, data: $0) } }
    }
}

extension BSONDocument {
    
    func combine(_ other: BSONDocument) -> BSONDocument {
        var copy = self
        for (key, value) in other {
            copy[key] = value
        }
        return copy
    }
}

extension ParseObject {
    
    public func save(to connection: DBConnection) -> EventLoopFuture<ParseObject> {
        
        if let id = data["_id"] {
            
            return connection.mongoQuery().collection(`class`).updateOne().filter(["_id": id]).update(["$set": BSON(updated)]).execute().flatMapThrowing { result in
                
                guard result == nil else { throw ParseError.unknown }
                
                return ParseObject(class: self.class, data: self.data.combine(self.updated))
            }
            
        } else {
            
            
            return connection.mongoQuery().collection(`class`).insertOne().value(updated).execute().flatMapThrowing { result in
                
                guard let result = result else { throw ParseError.unknown }
                
                var data = self.updated
                data["_id"] = result.insertedID
                
                return ParseObject(class: self.class, data: data)
            }
        }
    }
    
    public func delete(from connection: DBConnection) -> EventLoopFuture<Void> {
        guard let id = data["_id"] else { return connection.eventLoop.makeFailedFuture(ParseError.nullObjectId) }
        return connection.mongoQuery().collection(`class`).deleteOne().filter(["_id": id]).execute().map { _ in }
    }
}

extension ParseObject: Encodable {
    
    struct CodingKey: Swift.CodingKey {
        
        var stringValue: String
        
        var intValue: Int? { nil }
        
        init(stringValue: String) {
            self.stringValue = stringValue
        }
        
        init?(intValue: Int) {
            return nil
        }
    }
    
    public func encode(to encoder: Encoder) throws {
        
        var container = encoder.container(keyedBy: CodingKey.self)
        
        for key in keys {
            let encoder = ExtendedJSONEncoder()
            let data = try encoder.encode(self[key])
            try container.encode(Json(decode: data), forKey: CodingKey(stringValue: key))
        }
    }
}

extension ParseObject {
    
    func toPointer() -> String? {
        guard let id = self.id else { return nil }
        return "\(self.class)$\(id)"
    }
}

public func == (lhs: MongoPredicateKey, rhs: ParseObject) -> MongoPredicateExpression {
    return MongoPredicateKey(key: "_p_\(lhs.key)") == rhs.toPointer()
}

public func != (lhs: MongoPredicateKey, rhs: ParseObject) -> MongoPredicateExpression {
    return MongoPredicateKey(key: "_p_\(lhs.key)") != rhs.toPointer()
}

public func == (lhs: ParseObject, rhs: MongoPredicateKey) -> MongoPredicateExpression {
    return lhs.toPointer() == MongoPredicateKey(key: "_p_\(rhs.key)")
}

public func != (lhs: ParseObject, rhs: MongoPredicateKey) -> MongoPredicateExpression {
    return lhs.toPointer() != MongoPredicateKey(key: "_p_\(rhs.key)")
}
