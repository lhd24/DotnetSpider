using DotnetSpider.Core;
using DotnetSpider.Core.Infrastructure;
using DotnetSpider.Core.Scheduler;
using DotnetSpider.Core.Scheduler.Component;
using MongoDB.Bson;
using MongoDB.Driver;
using System;
using System.Collections.Generic;
using System.Linq;

namespace Eastday
{
    public class MongoScheduler : DuplicateRemovedScheduler, IDuplicateRemover
    {
        private List<Request> _queue = new List<Request>();
        IMongoDatabase _mongoDb;
        private readonly AutomicLong _successCounter = new AutomicLong(0);
        private readonly AutomicLong _errorCounter = new AutomicLong(0);

        public MongoScheduler()
        {
            DuplicateRemover = this;
            var client = new MongoClient("mongodb://localhost:27017");
            _mongoDb = client.GetDatabase("Eastday");
            _urls = _mongoDb.GetCollection<BsonDocument>("Spider");
        }

        private readonly IMongoCollection<BsonDocument> _urls;

        public bool IsDuplicate(Request request)
        {
            bool isDuplicate = _urls.Find(Builders<BsonDocument>.Filter.Eq("_id", request.Identity)).Any();
            if (!isDuplicate)
            {
                _urls.InsertOne(new BsonDocument { { "_id", request.Identity } });
            }
            return isDuplicate;
        }

        public override void ResetDuplicateCheck()
        {
            lock (this)
            {
                _queue.Clear();
            }
        }

        protected override void PushWhenNoDuplicate(Request request)
        {
            // ReSharper disable once InconsistentlySynchronizedField
            _queue.Add(request);
        }

        public override Request Poll()
        {
            lock (this)
            {
                if (_queue.Count == 0)
                {
                    return null;
                }
                else
                {
                    Request request;
                    if (DepthFirst)
                    {
                        request = _queue.Last();
                        _queue.RemoveAt(_queue.Count - 1);
                    }
                    else
                    {
                        request = _queue.First();
                        _queue.RemoveAt(0);
                    }

                    return request;
                }
            }
        }

        public override long GetTotalRequestsCount()
        {
            return _urls.Count(FilterDefinition<BsonDocument>.Empty);
        }

        public override void Import(HashSet<Request> requests)
        {
            lock (this)
            {
                _queue = new List<Request>(requests);
            }
        }

        public override HashSet<Request> ToList()
        {
            lock (this)
            {
                return new HashSet<Request>(_queue.ToArray());
            }
        }

        public override long GetLeftRequestsCount()
        {
            lock (this)
            {
                return _queue.Count;
            }
        }

        public override long GetSuccessRequestsCount()
        {
            return _successCounter.Value;
        }

        public override long GetErrorRequestsCount()
        {
            return _errorCounter.Value;
        }

        public override void IncreaseSuccessCounter()
        {
            _successCounter.Inc();
        }

        public override void IncreaseErrorCounter()
        {
            _errorCounter.Inc();
        }

        public override void Dispose()
        {
            _queue.Clear();
            base.Dispose();
        }
    }
}
