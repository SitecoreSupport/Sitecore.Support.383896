using System.Collections.Generic;
using System.Linq;
using System.Runtime.Serialization;
using Sitecore.ContentSearch;
using Sitecore.ContentSearch.Abstractions;
using Sitecore.ContentSearch.Diagnostics;
using Sitecore.ContentSearch.Maintenance;
using Sitecore.ContentSearch.Utilities;
using Sitecore.Data;
using Sitecore.Data.Archiving;
using Sitecore.Data.Managers;
using Sitecore.Diagnostics;

namespace Sitecore.Support.ContentSearch.Maintenance.Strategies
{
    [DataContract]
    public class IntervalAsynchronousStrategy : Sitecore.ContentSearch.Maintenance.Strategies.IntervalAsynchronousStrategy, ISearchIndexInitializable
    {
        public IntervalAsynchronousStrategy(string database, string interval = null) : base(database, interval)
        {
        }

        ISearchIndex index;

        void ISearchIndexInitializable.Initialize(ISearchIndex index)
        {
            base.Initialize(index);
            
            this.index = index;
            this.InitializeSubscribers();
        }

        protected virtual void InitializeSubscribers()
        {
            if (!ContentSearchManager.Locator.GetInstance<IContentSearchConfigurationSettings>().ItemBucketsEnabled())
            {
                CrawlingLog.Log.Warn("SUPPORT Subscribers have not been initialized since the ItemBuckets functionality is disabled");
                return;
            }

            var instance = ContentSearchManager.Locator.GetInstance<IEventManager>();
            instance.Subscribe<RestoreVersionCompletedEvent>(this.OnRestoreVersionCompletedHandler);
            instance.Subscribe<RestoreItemCompletedEvent>(this.OnRestoreItemCompletedHandler);
        }

        protected virtual void OnRestoreVersionCompletedHandler(RestoreVersionCompletedEvent restoreVersionCompletedEvent)
        {
            if (!this.Database.Name.Equals(restoreVersionCompletedEvent.DatabaseName))
            {
                return;
            }

            var itemUri = new ItemUri(
                new ID(restoreVersionCompletedEvent.ItemId),
                LanguageManager.GetLanguage(restoreVersionCompletedEvent.Language),
                new Sitecore.Data.Version(restoreVersionCompletedEvent.Version),
                Database.GetDatabase(restoreVersionCompletedEvent.DatabaseName));
            var list = new List<SitecoreItemUniqueId>(1)
            {
                (SitecoreItemUniqueId)itemUri
            };

            this.Run(list);
        }

        protected virtual void OnRestoreItemCompletedHandler(RestoreItemCompletedEvent restoreItemCompletedEvent)
        {
            if (!this.Database.Name.Equals(restoreItemCompletedEvent.DatabaseName))
            {
                return;
            }

            ID itemId = new ID(restoreItemCompletedEvent.ItemId);

            var batch = Database.GetDatabase(restoreItemCompletedEvent.DatabaseName).GetItem(itemId).Versions.GetVersions(true).Select(it => (SitecoreItemUniqueId)it.Uri).ToList();
            this.Run(batch);
        }

        public void Run(List<SitecoreItemUniqueId> itemUris)
        {
            Assert.ArgumentNotNull(itemUris, "itemUris");

            if (IndexCustodian.IsIndexingPaused(this.index))
            {
                CrawlingLog.Log.Warn(string.Format("SUPPORT [Index={0}] The Strategy is disabled while indexing is paused.", this.index.Name));
                return;
            }

            if (BulkUpdateContext.IsActive)
            {
                CrawlingLog.Log.Debug("SUPPORT The strategy is disabled during BulkUpdateContext");
                return;
            }

            if (itemUris.Count == 0)
            {
                CrawlingLog.Log.Debug("SUPPORT A collection is empty");
                return;
            }

            if (itemUris.Count == 1)
            {
                var itemUri = itemUris.First();
                CrawlingLog.Log.Debug(string.Format("SUPPORT Update for update for '{0}' has been quequed...", itemUri));
                IndexCustodian.UpdateItem(this.index, itemUri);
                return;
            }

            CrawlingLog.Log.Debug(string.Format("SUPPORT Update for a batch ({0}) has been quequed...", itemUris.Count));
            IndexCustodian.IncrementalUpdate(this.index, itemUris);
        }
    }
}