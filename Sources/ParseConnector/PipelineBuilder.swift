
extension DBMongoPipelineBuilder {
    
    public func include(_ pointer: String, class className: String) -> Self {
        return self.appendStage([
            "$lookup": [
                "as": pointer.toBSON(),
                "from": className.toBSON(),
                "let": [pointer: "$_p_\(pointer)".toBSON()],
                "pipeline": [
                    [
                        "$match": [
                            "$expr": [
                                "$eq": ["$_id", ["$substr": ["$$\(pointer)".toBSON(), (pointer.count + 1).toBSON(), -1]]]
                            ]
                        ]
                    ]
                ]
            ]
        ]).appendStage([
            "$project": [
                pointer: ["$arrayElemAt": ["$\(pointer)".toBSON(), 0]]
            ]
        ])
    }
}
