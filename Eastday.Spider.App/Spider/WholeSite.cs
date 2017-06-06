using DotnetSpider.Core;
using DotnetSpider.Core.Pipeline;
using DotnetSpider.Core.Processor;
using DotnetSpider.Core.Selector;
using DotnetSpider.Extension.Scheduler;
using System;
using System.Collections.Generic;
using System.Configuration;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;

namespace Eastday
{
    public class WholeSite
    {
        public static void CrawlerPagesTraversal()
        {
            string redisConfig = ConfigurationManager.AppSettings["redis"];
            Site site = new Site
            {
                RemoveOutboundLinks = true
            };
            site.AddStartUrl("http://www.eastday.com/");
            Spider spider = Spider.Create(site, "eastday_whole", new RedisScheduler(redisConfig), new IPageProcessor[]
            {
                new WholeSiteProcessor()
            })
            .AddPipeline(new SitePipeline())
            .SetThreadNum(4);
            spider.EmptySleepTime = 30000;
            spider.Scheduler.OnPush += new EventHandler<Request>(WholeSite.Scheduler_OnPush);
            spider.Run();
        }

        private static void Scheduler_OnPush(object sender, Request request)
        {
            MongoConfiguration mongo = new MongoConfiguration(ConfigurationManager.AppSettings["mongodb"]);
            mongo.InsertUrlReferer(request);
        }
    }

    public class WholeSiteEntity
    {
        public string Identity
        {
            get;
            set;
        }

        public string Title
        {
            get;
            set;
        }

        public string Domain
        {
            get;
            set;
        }

        public string Url
        {
            get;
            set;
        }

        public override string ToString()
        {
            return string.Format("{0}|{1}|{2}", this.Identity, this.Url, this.Title);
        }
    }

    internal class WholeSiteProcessor : BasePageProcessor
    {
        private string[] excludeUrl = new string[]
        {
            "^https?://.+\\.pdf",
            "^https?://.+\\.jpg"
        };

        protected override void ExtractUrls(Page page)
        {
            base.ExtractUrls(page);
        }

        public WholeSiteProcessor()
        {
            this.AddTargetUrlExtractor(null, new string[]
            {
                "^https?://.+\\.eastday\\.com"
            });
            this.AddExcludeTargetUrlPattern(this.excludeUrl);
        }

        protected override void Handle(Page page)
        {
            Match regex = Regex.Match(page.TargetUrl, "^https?://(.+)\\.eastday\\.com", RegexOptions.IgnoreCase);
            bool success = regex.Success;
            if (success)
            {
                WholeSiteEntity news = new WholeSiteEntity
                {
                    Identity = page.Request.Identity,
                    Url = page.TargetUrl,
                    Domain = regex.Groups[1].Value,
                    Title = (page.Selectable.Select(Selectors.XPath("//title")).GetValue(false) ?? "")
                };
                page.AddResultItem("WholeStie", news);
            }
        }
    }

    internal class SitePipeline : BasePipeline
    {
        public override void Process(params ResultItems[] resultItems)
        {
            MongoConfiguration mongo = new MongoConfiguration(ConfigurationManager.AppSettings["mongodb"]);
            foreach (var resultItem in resultItems)
            {
                var news = resultItem.GetResultItem("WholeStie");
                if (news != null)
                {
                    mongo.InsertUrlDetail(news);
                }
            }
        }
    }
}
