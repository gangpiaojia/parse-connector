
extension DBConnection {
    
    public func parseQuery() -> ParseQuery {
        return ParseQuery(connection: self, session: nil, class: nil, filters: [], sort: nil, limit: nil)
    }
}

public struct ParseQuery {
    
    let connection: DBConnection
    
    let session: ClientSession?
    
    let `class`: String?
    
    let filters: [MongoPredicateExpression]
    
    let sort: BSONDocument?
    
    let limit: Int?
}

extension ParseQuery {
    
    public var eventLoop: EventLoop {
        return connection.eventLoop
    }
}

extension ParseQuery {
    
    public func `class`(_ class: String) -> ParseQuery {
        return ParseQuery(connection: connection, session: session, class: `class`, filters: filters, sort: sort, limit: nil)
    }
}

extension ParseQuery {
    
    private func _withSession(_ session: ClientSession) -> ParseQuery {
        return ParseQuery(connection: connection, session: session, class: `class`, filters: filters, sort: sort, limit: nil)
    }
    
    public func withSession<T>(
        options: ClientSessionOptions? = nil,
        _ sessionBody: (ParseQuery) throws -> EventLoopFuture<T>
    ) -> EventLoopFuture<T> {
        return connection.mongoQuery().withSession(options: options) { try sessionBody(self._withSession($0)) }
    }
    
    public func withTransaction<T>(
        options: ClientSessionOptions? = nil,
        _ transactionBody: @escaping (ParseQuery) throws -> EventLoopFuture<T>
    ) -> EventLoopFuture<T> {
        return connection.mongoQuery().withTransaction(options: options) { try transactionBody(self._withSession($0)) }
    }
}

extension ParseQuery {
    
    public func filter(
        _ predicate: (MongoPredicateBuilder) -> MongoPredicateExpression
    ) -> ParseQuery {
        return ParseQuery(connection: connection, session: session, class: `class`, filters: filters + [predicate(.init())], sort: sort, limit: nil)
    }
}

extension ParseQuery {
    
    public func sort(_ sort: BSONDocument) -> ParseQuery {
        return ParseQuery(connection: connection, session: session, class: `class`, filters: filters, sort: sort, limit: nil)
    }
    
    public func sort(_ sort: OrderedDictionary<String, DBMongoSortOrder>) -> ParseQuery {
        return ParseQuery(connection: connection, session: session, class: `class`, filters: filters, sort: sort.toBSONDocument(), limit: nil)
    }
    
    public func ascending(_ keys: String ...) -> ParseQuery {
        var sort: OrderedDictionary<String, DBMongoSortOrder> = [:]
        for key in keys {
            sort[key] = .ascending
        }
        return self.sort(sort)
    }
    
    public func descending(_ keys: String ...) -> ParseQuery {
        var sort: OrderedDictionary<String, DBMongoSortOrder> = [:]
        for key in keys {
            sort[key] = .descending
        }
        return self.sort(sort)
    }
}

extension ParseQuery {
    
    public func limit(_ limit: Int) -> ParseQuery {
        return ParseQuery(connection: connection, session: session, class: `class`, filters: filters, sort: sort, limit: limit)
    }
}

extension ParseQuery {
    
    func mongoQuery() -> DBMongoQuery {
        if let session = self.session {
            return connection.mongoQuery().session(session)
        }
        return connection.mongoQuery()
    }
}

extension ParseQuery {
    
    public func count() -> EventLoopFuture<Int> {
        
        do {
            
            guard let `class` = self.class else { throw ParseError.classNotSet }
            
            let filter = try self.filterBSONDocument(useExpr: true)
            
            return self.mongoQuery().collection(`class`).count().filter(filter).execute()
            
        } catch let error {
            
            return connection.eventLoop.makeFailedFuture(error)
        }
    }
}

extension ParseQuery {
    
    func filterBSONDocument(useExpr: Bool) throws -> BSONDocument {
        return try filters.reduce { $0 && $1 }?.toBSONDocument(useExpr: useExpr) ?? [:]
    }
}

extension ParseQuery {
    
    public func findOne() -> EventLoopFuture<ParseObject?> {
        
        do {
            
            guard let `class` = self.class else { throw ParseError.classNotSet }
            
            let filter = try self.filterBSONDocument(useExpr: false)
            
            var query = self.mongoQuery().collection(`class`).findOne().filter(filter)
            
            if let sort = self.sort {
                query = query.sort(sort)
            }
            
            return query.execute().map { $0.map { ParseObject(class: `class`, data: $0) } }
            
        } catch let error {
            
            return connection.eventLoop.makeFailedFuture(error)
        }
    }
    
    public func findOneAndUpdate(_ update: [String: BSON], upsert: Bool = false, returnDocument: ReturnDocument = .after) -> EventLoopFuture<ParseObject?> {
        return findOneAndUpdate(update.mapValues { .set($0) }, upsert: upsert, returnDocument: returnDocument)
    }
    
    public func findOneAndUpdate(_ update: [String: ParseUpdateOperation], upsert: Bool = false, returnDocument: ReturnDocument = .after) -> EventLoopFuture<ParseObject?> {
        
        do {
            
            guard let `class` = self.class else { throw ParseError.classNotSet }
            
            let filter = try self.filterBSONDocument(useExpr: false)
            
            let now = Date().toBSON()
            
            var update = update
            update["_updated_at"] = .set(now)
            
            if let _acl = update["_acl"]?.value {
                let acl = ParseACL(acl: _acl)
                update["_rperm"] = .set(acl.rperm.toBSON())
                update["_wperm"] = .set(acl.wperm.toBSON())
            }
            
            var _update = try update.toBSONDocument()
            
            if upsert {
                
                var setOnInsert: BSONDocument = [:]
                setOnInsert["_id"] = BSONObjectID().hex.toBSON()
                setOnInsert["_created_at"] = now
                
                if update["_acl"]?.value == nil {
                    setOnInsert["_acl"] = [:]
                    setOnInsert["_rperm"] = []
                    setOnInsert["_wperm"] = []
                }
                
                _update["$setOnInsert"] = BSON(setOnInsert)
            }
            
            var query = self.mongoQuery().collection(`class`).findOneAndUpdate().filter(filter).update(_update).upsert(upsert).returnDocument(returnDocument)
            
            if let sort = self.sort {
                query = query.sort(sort)
            }
            
            return query.execute().map { $0.map { ParseObject(class: `class`, data: $0) } }
            
        } catch let error {
            
            return connection.eventLoop.makeFailedFuture(error)
        }
    }
    
    public func findOneAndDelete() -> EventLoopFuture<ParseObject?> {
        
        do {
            
            guard let `class` = self.class else { throw ParseError.classNotSet }
            
            let filter = try self.filterBSONDocument(useExpr: false)
            
            var query = self.mongoQuery().collection(`class`).findOneAndDelete().filter(filter)
            
            if let sort = self.sort {
                query = query.sort(sort)
            }
            
            return query.execute().map { $0.map { ParseObject(class: `class`, data: $0) } }
            
        } catch let error {
            
            return connection.eventLoop.makeFailedFuture(error)
        }
    }
    
}

extension ParseQuery {
    
    func execute() -> EventLoopFuture<MongoCursor<BSONDocument>> {
        
        do {
            
            guard let `class` = self.class else { throw ParseError.classNotSet }
            
            let filter = try self.filterBSONDocument(useExpr: false)
            
            var query = self.mongoQuery().collection(`class`).find().filter(filter)
            
            if let sort = self.sort {
                query = query.sort(sort)
            }
            
            if let limit = self.limit {
                query = query.limit(limit)
            }
            
            return query.execute()
            
        } catch let error {
            
            return connection.eventLoop.makeFailedFuture(error)
        }
    }
    
    public func toArray() -> EventLoopFuture<[ParseObject]> {
        guard let `class` = self.class else { return connection.eventLoop.makeFailedFuture(ParseError.classNotSet) }
        return self.execute().toArray().map { $0.map { ParseObject(class: `class`, data: $0) } }
    }
    
    public func forEach(_ body: @escaping (ParseObject) throws -> Void) -> EventLoopFuture<Void> {
        guard let `class` = self.class else { return connection.eventLoop.makeFailedFuture(ParseError.classNotSet) }
        return self.execute().forEach { try body(ParseObject(class: `class`, data: $0)) }
    }
}

extension ParseQuery {
    
    public func pipeline(_ pipeline: [BSONDocument]) -> EventLoopFuture<[BSONDocument]> {
        
        do {
            
            guard let `class` = self.class else { throw ParseError.classNotSet }
            
            var query: [BSONDocument] = []
            
            let filter = try self.filterBSONDocument(useExpr: true)
            if !filter.isEmpty {
                query.append(["$match": .document(filter)])
            }
            if let sort = self.sort {
                query.append(["$sort": .document(sort)])
            }
            if let limit = self.limit {
                query.append(["$limit": .int64(Int64(limit))])
            }
            
            return self.mongoQuery().collection(`class`).aggregate().pipeline(query + pipeline).execute().toArray()
            
        } catch let error {
            
            return connection.eventLoop.makeFailedFuture(error)
        }
    }
    
    public func pipeline(asClass class: String, _ pipeline: [BSONDocument]) -> EventLoopFuture<[ParseObject]> {
        
        return self.pipeline(pipeline).map { $0.map { ParseObject(class: `class`, data: $0) } }
    }
    
    public func pipeline<OutputType: Codable>(as outputType: OutputType.Type, _ pipeline: [BSONDocument]) -> EventLoopFuture<[OutputType]> {
        
        do {
            
            guard let `class` = self.class else { throw ParseError.classNotSet }
            
            var query: [BSONDocument] = []
            
            let filter = try self.filterBSONDocument(useExpr: true)
            if !filter.isEmpty {
                query.append(["$match": .document(filter)])
            }
            if let sort = self.sort {
                query.append(["$sort": .document(sort)])
            }
            if let limit = self.limit {
                query.append(["$limit": .int64(Int64(limit))])
            }
            
            return self.mongoQuery().collection(`class`).aggregate().pipeline(query + pipeline).execute(as: outputType).toArray()
            
        } catch let error {
            
            return connection.eventLoop.makeFailedFuture(error)
        }
    }
}

extension ParseQuery {
    
    public typealias PipelineBuilder = (DBMongoAggregateExpression<BSONDocument>) throws -> DBMongoAggregateExpression<BSONDocument>
    
    public func pipeline(_ builder: PipelineBuilder) -> EventLoopFuture<[BSONDocument]> {
        
        do {
            
            guard let `class` = self.class else { throw ParseError.classNotSet }
            
            var query = self.mongoQuery().collection(`class`).aggregate()
            
            let filter = try self.filterBSONDocument(useExpr: true)
            if !filter.isEmpty {
                query = query.match(filter)
            }
            if let sort = self.sort {
                query = query.sort(sort)
            }
            if let limit = self.limit {
                query = query.limit(limit)
            }
            
            query = try builder(query)
            
            return query.execute().toArray()
            
        } catch let error {
            
            return connection.eventLoop.makeFailedFuture(error)
        }
    }
    
    public func pipeline(asClass class: String, _ builder: PipelineBuilder) -> EventLoopFuture<[ParseObject]> {
        
        return self.pipeline(builder).map { $0.map { ParseObject(class: `class`, data: $0) } }
    }
    
    public func pipeline<OutputType: Codable>(as outputType: OutputType.Type, _ builder: PipelineBuilder) -> EventLoopFuture<[OutputType]> {
        
        do {
            
            guard let `class` = self.class else { throw ParseError.classNotSet }
            
            var query = self.mongoQuery().collection(`class`).aggregate()
            
            let filter = try self.filterBSONDocument(useExpr: true)
            if !filter.isEmpty {
                query = query.match(filter)
            }
            if let sort = self.sort {
                query = query.sort(sort)
            }
            if let limit = self.limit {
                query = query.limit(limit)
            }
            
            query = try builder(query)
            
            return query.execute(as: outputType).toArray()
            
        } catch let error {
            
            return connection.eventLoop.makeFailedFuture(error)
        }
    }
}
