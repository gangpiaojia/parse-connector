
import MongoSwift

extension DBConnection {
    
    public func parseQuery() -> ParseQuery {
        return ParseQuery(connection: self, session: nil, class: nil, filters: [], limit: nil)
    }
}

public struct ParseQuery {
    
    let connection: DBConnection
    
    let session: ClientSession?
    
    let `class`: String?
    
    let filters: [ParsePredicateExpression]
    
    let limit: Int?
}

extension ParseQuery {
    
    public func `class`(_ class: String) -> ParseQuery {
        return ParseQuery(connection: connection, session: session, class: `class`, filters: filters, limit: nil)
    }
}

extension ParseQuery {
    
    private func _withSession(_ session: ClientSession) -> ParseQuery {
        return ParseQuery(connection: connection, session: session, class: `class`, filters: filters, limit: nil)
    }
    
    public func withSession<T>(
        options: ClientSessionOptions? = nil,
        _ sessionBody: (ParseQuery) throws -> EventLoopFuture<T>
    ) -> EventLoopFuture<T> {
        return connection.mongoQuery().withSession(options: options) { try sessionBody(self._withSession($0)) }
    }
}

extension ParseQuery {
    
    public func filter(
        _ predicate: (ParsePredicateBuilder) -> ParsePredicateExpression
    ) -> ParseQuery {
        return ParseQuery(connection: connection, session: session, class: `class`, filters: filters + [predicate(.init())], limit: nil)
    }
}

extension ParseQuery {
    
    public func limit(_ limit: Int) -> ParseQuery {
        return ParseQuery(connection: connection, session: session, class: `class`, filters: filters, limit: limit)
    }
}

extension ParseQuery {
    
    public func count() -> EventLoopFuture<Int> {
        
        do {
            
            guard let `class` = self.class else { throw ParseError.classNotSet }
            
            let filter: BSONDocument
            
            switch filters.count {
            case 0: filter = [:]
            case 1: filter = try filters[0].toBSONDocument()
            default: filter = try ["$and": BSON(filters.map { try $0.toBSONDocument() })]
            }
            
            return connection.mongoQuery().collection(`class`).count().filter(filter).execute()
            
        } catch let error {
            
            return connection.eventLoop.makeFailedFuture(error)
        }
    }
}

extension ParseQuery {
    
    public func findOne() -> EventLoopFuture<ParseObject?> {
        
        do {
            
            guard let `class` = self.class else { throw ParseError.classNotSet }
            
            let filter: BSONDocument
            
            switch filters.count {
            case 0: filter = [:]
            case 1: filter = try filters[0].toBSONDocument()
            default: filter = try ["$and": BSON(filters.map { try $0.toBSONDocument() })]
            }
            
            let query = connection.mongoQuery().collection(`class`).findOne().filter(filter)
            
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
            
            let filter: BSONDocument
            
            switch filters.count {
            case 0: filter = [:]
            case 1: filter = try filters[0].toBSONDocument()
            default: filter = try ["$and": BSON(filters.map { try $0.toBSONDocument() })]
            }
            
            var query = connection.mongoQuery().collection(`class`).find().filter(filter)
            
            if let limit = limit {
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
