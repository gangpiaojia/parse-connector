
public struct ParseACL {
    
    var acl: BSONDocument
    
    init(acl: BSON) {
        self.acl = acl.documentValue ?? ["*": ["r": true, "w": true]]
    }
    
    init(acl: BSONDocument) {
        self.acl = acl
    }
    
    public init() {
        self.acl = ["*": ["r": true, "w": true]]
    }
}

extension ParseACL {
    
    public var rperm: [String] {
        return acl.filter { $0.value.documentValue?["r"]?.boolValue == true }.keys
    }
    
    public var wperm: [String] {
        return acl.filter { $0.value.documentValue?["w"]?.boolValue == true }.keys
    }
}

extension ParseACL {
    
    public var `public`: Perm {
        get {
            return Perm(acl["*"] ?? [:])
        }
        set {
            if newValue.read || newValue.write {
                acl["*"] = newValue.toBSON()
            } else {
                acl["*"] = nil
            }
        }
    }
    
    public subscript(_ userId: String) -> Perm {
        get {
            return Perm(acl[userId] ?? [:])
        }
        set {
            if newValue.read || newValue.write {
                acl[userId] = newValue.toBSON()
            } else {
                acl[userId] = nil
            }
        }
    }
}

extension ParseACL {
    
    public struct Perm {
        
        public var read: Bool
        
        public var write: Bool
        
        init(_ perm: BSON) {
            self.init(perm.documentValue ?? [:])
        }
        
        init(_ perm: BSONDocument) {
            self.read = perm["r"]?.boolValue == true
            self.write = perm["w"]?.boolValue == true
        }
    }
}

extension ParseACL: BSONConvertible {
    
    public func toBSON() -> BSON {
        return acl.toBSON()
    }
}

extension ParseACL.Perm: BSONConvertible {
    
    public func toBSON() -> BSON {
        var perm: [String: Bool] = [:]
        if read { perm["r"] = true }
        if write { perm["w"] = true }
        return perm.toBSON()
    }
}
