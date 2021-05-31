
public struct ParseObject {
    
    public let `class`: String
    
    var data: BSONDocument
    
    var mutated: [String: ParseUpdateOperation] = [:]
    
    init(class: String, data: BSONDocument) {
        self.class = `class`
        self.data = data
    }
    
    public init(class: String, id: String? = nil) {
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
    
    public var acl: ParseACL {
        get {
            return ParseACL(acl: self["_acl"])
        }
        set {
            self["_acl"] = newValue.acl.toBSON()
        }
    }
    
    public var keys: [String] {
        let keys = data.keys + mutated.keys
        return Array(keys.uniqued())
    }
    
    public subscript(_ key: String) -> BSON {
        get {
            return mutated[key]?.value ?? data[key] ?? .undefined
        }
        set {
            guard key != "_id" else { fatalError("_id is not writable") }
            mutated[key] = .set(newValue)
        }
    }
}

extension ParseObject {
    
    public mutating func set<T: BSONConvertible>(_ key: String, _ value: T) {
        self[key] = value.toBSON()
    }
    
    public mutating func set(_ key: String, _ object: ParseObject?) {
        self["_p_\(key)"] = (object?.toPointer()).toBSON()
    }
    
    public mutating func increment<T: BSONConvertible & Numeric>(_ key: String, by amount: T) {
        mutated[key] = .increment(amount.toBSON())
    }
    
    public mutating func multiply<T: BSONConvertible & Numeric>(_ key: String, by amount: T) {
        mutated[key] = .multiply(amount.toBSON())
    }
    
    public mutating func max<T: BSONConvertible>(_ key: String, by value: T) {
        mutated[key] = .max(value.toBSON())
    }
    
    public mutating func min<T: BSONConvertible>(_ key: String, by value: T) {
        mutated[key] = .min(value.toBSON())
    }
    
    public mutating func addToSet<T: BSONConvertible>(_ key: String, with value: T) {
        mutated[key] = .addToSet(value.toBSON())
    }
    
    public mutating func push<T: BSONConvertible>(_ key: String, with value: T) {
        mutated[key] = .push(value.toBSON())
    }
    
    public mutating func removeAll<T: BSONConvertible>(_ key: String, values: [T]) {
        mutated[key] = .pullAll(values.map { $0.toBSON() })
    }
    
    public mutating func popFirst(for key: String) {
        mutated[key] = .popFirst
    }
    
    public mutating func popLast(for key: String) {
        mutated[key] = .popLast
    }
}

extension ParseObject {
    
    private func _fetch<S: Sequence>(_ query: DBMongoQuery, keys: S, on eventLoop: EventLoop) -> EventLoopFuture<ParseObject?> where S.Element == String {
        
        guard let id = data["_id"] else { return eventLoop.makeFailedFuture(ParseError.nullObjectId) }
        
        var query = query.collection(`class`).findOne().filter(["_id": id])
        
        let projection = Dictionary(uniqueKeysWithValues: keys.map { ($0, 1) })
        if !projection.isEmpty {
            query = query.projection(BSONDocument(projection))
        }
        
        return query.execute().map { $0.map { ParseObject(class: `class`, data: $0) } }
    }
    
    public func fetch(_ keys: String..., from connection: DBConnection) -> EventLoopFuture<ParseObject?> {
        return self._fetch(connection.mongoQuery(), keys: keys, on: connection.eventLoopGroup.next())
    }
    
    public func fetch(_ keys: String..., from connection: ParseQuery) -> EventLoopFuture<ParseObject?> {
        return self._fetch(connection.mongoQuery(), keys: keys, on: connection.eventLoopGroup.next())
    }
    
    public func fetch<S: Sequence>(_ keys: S, from connection: DBConnection) -> EventLoopFuture<ParseObject?> where S.Element == String {
        return self._fetch(connection.mongoQuery(), keys: keys, on: connection.eventLoopGroup.next())
    }
    
    public func fetch<S: Sequence>(_ keys: S, from connection: ParseQuery) -> EventLoopFuture<ParseObject?> where S.Element == String {
        return self._fetch(connection.mongoQuery(), keys: keys, on: connection.eventLoopGroup.next())
    }
}

extension ParseObject {
    
    private func _save(_ query: DBMongoQuery) -> EventLoopFuture<ParseObject> {
        
        do {
            
            if let id = data["_id"] {
                
                var mutated = self.mutated
                mutated["_updated_at"] = .set(Date().toBSON())
                
                if let _acl = self.mutated["_acl"]?.value {
                    let acl = ParseACL(acl: _acl)
                    mutated["_rperm"] = .set(acl.rperm.toBSON())
                    mutated["_wperm"] = .set(acl.wperm.toBSON())
                }
                
                return try query.collection(`class`).findOneAndUpdate().filter(["_id": id]).update(mutated.toBSONDocument()).returnDocument(.after).execute().flatMapThrowing { result in
                    
                    guard let result = result else { throw ParseError.objectNotFound }
                    
                    return ParseObject(class: `class`, data: result)
                }
                
            } else {
                
                let now = Date().toBSON()
                
                var values = self.mutated.compactMapValues { $0.value }
                
                values["_id"] = BSONObjectID().hex.toBSON()
                values["_created_at"] = now
                values["_updated_at"] = now
                
                let acl = ParseACL(acl: values["_acl"] ?? [:])
                values["_rperm"] = acl.rperm.toBSON()
                values["_wperm"] = acl.wperm.toBSON()
                
                return query.collection(`class`).insertOne().value(BSONDocument(values)).execute().flatMapThrowing { result in
                    
                    guard let result = result else { throw ParseError.unknown }
                    
                    var data = values
                    data["_id"] = result.insertedID
                    
                    return ParseObject(class: self.class, data: BSONDocument(data))
                }
            }
            
        } catch let error {
            
            return query.eventLoopGroup.next().makeFailedFuture(error)
        }
    }
    
    public func save(to connection: DBConnection) -> EventLoopFuture<ParseObject> {
        return self._save(connection.mongoQuery())
    }
    
    public func save(to connection: ParseQuery) -> EventLoopFuture<ParseObject> {
        return self._save(connection.mongoQuery())
    }
}

extension ParseObject {
    
    private func _delete(_ query: DBMongoQuery, on eventLoop: EventLoop) -> EventLoopFuture<Void> {
        guard let id = data["_id"] else { return eventLoop.makeFailedFuture(ParseError.nullObjectId) }
        return query.collection(`class`).deleteOne().filter(["_id": id]).execute().map { _ in }
    }
    
    public func delete(from connection: DBConnection) -> EventLoopFuture<Void> {
        return self._delete(connection.mongoQuery(), on: connection.eventLoopGroup.next())
    }
    
    public func delete(from connection: ParseQuery) -> EventLoopFuture<Void> {
        return self._delete(connection.mongoQuery(), on: connection.eventLoopGroup.next())
    }
}

extension ParseObject {
    
    public func toPointer() -> String? {
        guard let id = self.id else { return nil }
        return "\(self.class)$\(id)"
    }
}

extension MongoPredicateBuilder {
    
    public var createdAt: MongoPredicateKey {
        return MongoPredicateKey(key: "_created_at")
    }
    
    public var updatedAt: MongoPredicateKey {
        return MongoPredicateKey(key: "_updated_at")
    }
}

public func == (lhs: MongoPredicateKey, rhs: ParseObject?) -> MongoPredicateExpression {
    return MongoPredicateKey(key: "_p_\(lhs.key)") == rhs?.toPointer()
}

public func != (lhs: MongoPredicateKey, rhs: ParseObject?) -> MongoPredicateExpression {
    return MongoPredicateKey(key: "_p_\(lhs.key)") != rhs?.toPointer()
}

public func == (lhs: ParseObject?, rhs: MongoPredicateKey) -> MongoPredicateExpression {
    return lhs?.toPointer() == MongoPredicateKey(key: "_p_\(rhs.key)")
}

public func != (lhs: ParseObject?, rhs: MongoPredicateKey) -> MongoPredicateExpression {
    return lhs?.toPointer() != MongoPredicateKey(key: "_p_\(rhs.key)")
}
