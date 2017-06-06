using DotnetSpider.Core;
using DotnetSpider.Core.Infrastructure;
using MongoDB.Bson;
using MongoDB.Driver;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Eastday
{
    class MongoConfiguration
    {
        private IMongoDatabase _mongoDb;

        private IMongoCollection<BsonDocument> Referer;

        private IMongoCollection<BsonDocument> Urls;

        public MongoConfiguration(string constr)
        {
            MongoClient client = new MongoClient(constr);
            this._mongoDb = client.GetDatabase("Eastday", null);
            this.Urls = this._mongoDb.GetCollection<BsonDocument>("Urls", null);
            this.Referer = this._mongoDb.GetCollection<BsonDocument>("Referer", null);
        }

        public void InsertUrlDetail(WholeSiteEntity site)
        {
            this.Urls.InsertOne(new BsonDocument
            {
                {
                    "_id",
                    site.Identity
                },
                {
                    "Domain",
                    site.Domain
                },
                {
                    "Url",
                    site.Url
                },
                {
                    "Title",
                    site.Title
                }
            });
        }

        public void InsertUrlReferer(Request request)
        {
            string mongoid = Encrypt.Md5Encrypt(request.Identity + request.GetExtra("RefererIdentity"));
            bool flag = !this.Referer.Find(Builders<BsonDocument>.Filter.Eq<string>("_id", mongoid), null).Any();
            if (flag)
            {
                BsonDocument bsonDocument = new BsonDocument();
                bsonDocument.Add("_id", mongoid);
                bsonDocument.Add("Url", request.Identity);
                bsonDocument.Add("Referer", request.GetExtra("RefererIdentity"));
                Referer.InsertOne(bsonDocument);
            }
        }
    }
}
