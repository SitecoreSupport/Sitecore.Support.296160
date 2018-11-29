using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Threading.Tasks;
using Sitecore.Analytics.Model;
using Sitecore.CES.DeviceDetection;
using Sitecore.Cintel;
using Sitecore.Cintel.Commons;
using Sitecore.Cintel.Reporting.Utility;
using Sitecore.Cintel.Search;
using Sitecore.ContentSearch.Linq.Utilities;
using Sitecore.XConnect;
using Sitecore.XConnect.Client.Configuration;
using Sitecore.XConnect.Collection.Model;

namespace Sitecore.Support.Cintel
{
  public class ContactSearchProvider : IContactSearchProvider
  {
    public byte[] Bookmark { get; set; }

    public ResultSet<List<IContactSearchResult>> Find(ContactSearchParameters parameters)
    {
      return Task.Run(async () => await FindAsync(parameters)).ConfigureAwait(false).GetAwaiter().GetResult();
    }

    private async Task<ResultSet<List<IContactSearchResult>>> FindAsync(ContactSearchParameters parameters)
    {
      var finalResultSet = new ResultSet<List<IContactSearchResult>>(parameters.PageNumber, parameters.PageSize);
      using (var client = SitecoreXConnectClientConfiguration.GetClient("xconnect/clientconfig"))
      {
        var facets = new string[3]
        {
          "EngagementMeasures",
          "Personal",
          "Emails"
        };
        var deviceFilters = parameters.AdditionalParameters.Keys.Contains("SearchDeviceFilters")
          ? parameters.AdditionalParameters["SearchDeviceFilters"] as List<SearchItem>
          : null;
        var batchEnumerator = QueryIndex(client.Contacts, parameters, facets);
        var num = await batchEnumerator.Result.MoveNext() ? 1 : 0;
        var current = batchEnumerator.Result.Current;
        Bookmark = batchEnumerator.Result.GetBookmark();
        finalResultSet.TotalResultCount = batchEnumerator.Result.TotalCount;
        var contactSearchResultList = new List<IContactSearchResult>();
        foreach (var contact in current)
        {
          var facet1 = contact.GetFacet<EngagementMeasures>("EngagementMeasures");
          var ipInfo = contact.Interactions.OfType<IpInfo>().FirstOrDefault();
          var facet2 = contact.GetFacet<PersonalInformation>("Personal");
          var facet3 = contact.GetFacet<EmailAddressList>("Emails");
          contact.Interactions.FirstOrDefault();
          var interaction = contact.Interactions.Count() < 0
            ? null
            : ApplyDeviceFilter(contact.Interactions, deviceFilters).OrderByDescending(o => o.StartDateTime)
              .FirstOrDefault();
          if (interaction != null)
          {
            var contactSearch = BuildBaseResult(contact, facet2, facet3, facet1);
            if (facet1 != null)
              PopulateLatestVisit(contact, interaction, ipInfo, ref contactSearch);
            contactSearchResultList.Add(contactSearch);
          }
        }

        finalResultSet.Data.Dataset.Add("ContactSearchResults", contactSearchResultList);
        deviceFilters = null;
        batchEnumerator = null;
      }

      return finalResultSet;
    }

    private Task<IAsyncEntityBatchEnumerator<Contact>> QueryIndex(IAsyncQueryable<Contact> contacts,
      ContactSearchParameters parameters, string[] facets)
    {
      var text = parameters.Match;
      if (parameters.PageNumber == 1)
        Bookmark = null;
      var interactionExpandOptions = new RelatedInteractionsExpandOptions("IpInfo", "ProfileScores", "UserAgentInfo")
      {
        StartDateTime = parameters.FromDate,
        EndDateTime = parameters.ToDate,
        Limit = int.MaxValue
      };
      var asyncQueryable = (IAsyncQueryable<Contact>)contacts.Where(x => x.InteractionsCache().InteractionCaches.Any(y => y.StartDateTime >= parameters.FromDate.ToUniversalTime() && y.StartDateTime <= parameters.ToDate.ToUniversalTime()))
        .OrderByDescending(c => c.EngagementMeasures().MostRecentInteractionStartDateTime);
      if (!string.IsNullOrEmpty(text) && !text.Equals("*"))
      {
        var predicate = ((Expression<Func<Contact, bool>>) (c => c.Personal().FirstName == text))
          .Or(c => CollectionModel.Personal(c).LastName == text).Or(c => c.Emails().PreferredEmail.SmtpAddress == text);
        asyncQueryable = asyncQueryable.Where(predicate);
      }

      var searchItems1 = parameters.AdditionalParameters.ContainsKey("SearchChannelFilters")
        ? parameters.AdditionalParameters["SearchChannelFilters"] as List<SearchItem>
        : null;
      if (searchItems1 != null && searchItems1.Count != 0)
      {
        var predicate = new ChannelFilters("SearchChannelFilters", searchItems1).BuildExpression();
        asyncQueryable = asyncQueryable.Where(predicate);
      }

      var searchItems2 = parameters.AdditionalParameters.ContainsKey("SearchOutcomeFilters")
        ? parameters.AdditionalParameters["SearchOutcomeFilters"] as List<SearchItem>
        : null;
      if (searchItems2 != null && searchItems2.Count != 0)
      {
        var predicate = new OutcomeFilters("SearchOutcomeFilters", searchItems2).BuildExpression();
        asyncQueryable = asyncQueryable.Where(predicate);
      }

      var searchItems3 = parameters.AdditionalParameters.ContainsKey("SearchGoalFilters")
        ? parameters.AdditionalParameters["SearchGoalFilters"] as List<SearchItem>
        : null;
      if (searchItems3 != null && searchItems3.Count != 0)
      {
        var predicate = new GoalFilters("SearchGoalFilters", searchItems3).BuildExpression();
        asyncQueryable = asyncQueryable.Where(predicate);
      }

      return GetResult(asyncQueryable, facets, interactionExpandOptions, parameters.PageSize);
    }

    private Task<IAsyncEntityBatchEnumerator<Contact>> GetResult(IAsyncQueryable<Contact> query, string[] facets,
      RelatedInteractionsExpandOptions interactionExpandOptions, int pageSize)
    {
      return query.WithExpandOptions(new ContactExpandOptions(facets)
      {
        Interactions = interactionExpandOptions
      }).GetBatchEnumerator(Bookmark, pageSize);
    }

    private static IContactSearchResult BuildBaseResult(Contact contact, PersonalInformation personalInformation,
      EmailAddressList emailAddressList, EngagementMeasures enaEngagementMeasures)
    {
      var identificationLevel =
        contact.Identifiers.Any(identifier => identifier.IdentifierType == ContactIdentifierType.Known)
          ? ContactIdentificationLevel.Known
          : ContactIdentificationLevel.Anonymous;
      return new ContactSearchResult
      {
        IdentificationLevel = (int) identificationLevel,
        ContactId = contact.Id.GetValueOrDefault(),
        FirstName = personalInformation?.FirstName,
        MiddleName = personalInformation?.MiddleName,
        Surname = personalInformation?.LastName,
        PreferredEmail = emailAddressList?.PreferredEmail?.SmtpAddress,
        JobTitle = personalInformation?.JobTitle,
        Value = enaEngagementMeasures != null ? enaEngagementMeasures.TotalValue : 0,
        VisitCount = enaEngagementMeasures != null ? enaEngagementMeasures.TotalInteractionCount : 0
      };
    }

    private static void PopulateLatestVisit(Contact contact, Interaction interaction, IpInfo ipInfo,
      ref IContactSearchResult contactSearch)
    {
      contactSearch.LatestVisitId = interaction.Id.GetValueOrDefault();
      contactSearch.LatestVisitStartDateTime = interaction.StartDateTime;
      contactSearch.LatestVisitEndDateTime = interaction.EndDateTime;
      contactSearch.LatestVisitPageViewCount = interaction.Events.OfType<PageViewEvent>().Count();
      contactSearch.LatestVisitValue = interaction.EngagementValue;
      var engagementMeasures = contact.EngagementMeasures();
      if (engagementMeasures != null)
        contactSearch.ValuePerVisit =
          Calculator.GetAverageValue(engagementMeasures.TotalValue, engagementMeasures.TotalInteractionCount);
      if (ipInfo == null)
        return;
      contactSearch.LatestVisitLocationCityDisplayName = ipInfo.City;
      contactSearch.LatestVisitLocationCountryDisplayName = ipInfo.Country;
      contactSearch.LatestVisitLocationRegionDisplayName = ipInfo.Region;
      contactSearch.LatestVisitLocationId = new Guid?();
    }

    private static IEnumerable<Interaction> ApplyDeviceFilter(IEnumerable<Interaction> filteredInteractions,
      List<SearchItem> deviceFilters)
    {
      if (deviceFilters != null && deviceFilters.Count != 0)
        filteredInteractions =
          filteredInteractions.Where(i => deviceFilters.Select(d => d.ItemId).Contains(GetDevicetype(i.UserAgent)));
      return filteredInteractions;
    }

    public static string GetDevicetype(string userAgent)
    {
      var str = "Unknown";
      if (DeviceDetectionManager.IsEnabled && DeviceDetectionManager.IsReady)
      {
        str = DeviceDetectionManager.GetDeviceInformation(userAgent).DeviceType.ToString();
      }
      else if (DeviceDetectionManager.IsEnabled && !DeviceDetectionManager.IsReady)
      {
        var num = 0;
        var timeout = new TimeSpan(3000L);
        while (DeviceDetectionManager.IsReady)
        {
          if (DeviceDetectionManager.IsReady)
          {
            str = DeviceDetectionManager.GetDeviceInformation(userAgent).DeviceType.ToString();
            break;
          }

          if (num != 4)
          {
            DeviceDetectionManager.CheckInitialization(timeout);
            ++num;
          }
          else
          {
            break;
          }
        }
      }

      return str;
    }
  }
}