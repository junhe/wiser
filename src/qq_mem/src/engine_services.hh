class DocumentStoreService {
    public:
        virtual void AddDocument(int id, std::string document) = 0;
        virtual void RemoveDocument(int id) = 0;
        virtual bool HasDocument(int id) = 0;
        virtual void Clear() = 0;
};


