using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Sitecore.Analytics.Model;
using Sitecore.Cintel.Commons;
using Sitecore.Cintel.Search;
using Sitecore.XConnect.Collection.Model;
using Sitecore.XConnect;
using System;
using Sitecore.ContentSearch.Linq.Utilities;
using System.Linq.Expressions;
using Sitecore.Cintel.Reporting.Utility;
using Sitecore.Cintel;

namespace Sitecore.Support.Cintel
{
  public class ContactSearchProvider : IContactSearchProvider
  {
    public byte[] Bookmark { get; set; }

    public ResultSet<List<IContactSearchResult>> Find(ContactSearchParameters parameters)
    {
      return Task.Run(async () => await FindAsync(parameters)).ConfigureAwait(false).GetAwaiter().GetResult();
    }


    #region private methods
    private async Task<ResultSet<List<IContactSearchResult>>> FindAsync(ContactSearchParameters parameters)
    {
      var finalResultSet = new ResultSet<List<IContactSearchResult>>(parameters.PageNumber, parameters.PageSize);
      using (var client = XConnect.Client.Configuration.SitecoreXConnectClientConfiguration.GetClient())
      {
        string[] facets = { EngagementMeasures.DefaultFacetKey, PersonalInformation.DefaultFacetKey, EmailAddressList.DefaultFacetKey };
        List<SearchItem> deviceFilters = parameters.AdditionalParameters.Keys.Contains(SearchFiltersParams.Device_Param) ?
            parameters.AdditionalParameters[SearchFiltersParams.Device_Param] as List<SearchItem> : null;

        var batchEnumerator = QueryIndex(client.Contacts, parameters, facets);

        await batchEnumerator.Result.MoveNext();

        var currentEnumerator = batchEnumerator.Result.Current;

        var contacts = currentEnumerator;
        Bookmark = batchEnumerator.Result.GetBookmark();
        finalResultSet.TotalResultCount = batchEnumerator.Result.TotalCount;

        var contactsList = new List<IContactSearchResult>();

        foreach (var sr in contacts)
        {
          var engagementInfo = sr.GetFacet<EngagementMeasures>(EngagementMeasures.DefaultFacetKey);
          var ipInfo = sr.Interactions.OfType<IpInfo>().FirstOrDefault();
          var personalInfo = sr.GetFacet<PersonalInformation>(PersonalInformation.DefaultFacetKey);
          var emailInfo = sr.GetFacet<EmailAddressList>(EmailAddressList.DefaultFacetKey);

          var interaction = sr.Interactions.FirstOrDefault();

          IEnumerable<Interaction> filteredInteractions = sr.Interactions;
          if (filteredInteractions.Count() >= 0)
          {
            filteredInteractions = ApplyDeviceFilter(filteredInteractions, deviceFilters);
            interaction = filteredInteractions.FirstOrDefault();
          }
          else
            interaction = null;

          if (interaction != null)
          {
            IContactSearchResult contact = BuildBaseResult(sr, personalInfo, emailInfo, engagementInfo);

            if (null != engagementInfo)
            {
              PopulateLatestVisit(sr, interaction, ipInfo, ref contact);
            }
            contactsList.Add(contact);
          }
        }

        finalResultSet.Data.Dataset.Add("ContactSearchResults", contactsList);
      }

      return finalResultSet;
    }

    private Task<IAsyncEntityBatchEnumerator<Contact>> QueryIndex(IAsyncQueryable<Contact> contacts,
        ContactSearchParameters parameters, string[] facets)
    {
      string text = parameters.Match;
      Expression<Func<Contact, bool>> predicateExpression;
      if (parameters.PageNumber == 1)
      {
        Bookmark = null;
      }

      var interactionExpandOptions = new RelatedInteractionsExpandOptions(IpInfo.DefaultFacetKey
          , ProfileScores.DefaultFacetKey
          , UserAgentInfo.DefaultFacetKey)
      {
        StartDateTime = parameters.FromDate,
        EndDateTime = parameters.ToDate,
        Limit = int.MaxValue
      };

      var list = contacts.Where(x => x.InteractionsCache().InteractionCaches.Any())
          .OrderByDescending(c => c.EngagementMeasures().MostRecentInteractionStartDateTime);

      IAsyncQueryable<Contact> query = list;

      if (!string.IsNullOrEmpty(text) && !text.Equals("*"))
      {
        predicateExpression = c => c.Personal().FirstName == text;
        predicateExpression = predicateExpression.Or(c => c.Personal().LastName == text);
        predicateExpression = predicateExpression.Or(c => c.Emails().PreferredEmail.SmtpAddress == text);
        query = query.Where(predicateExpression);
      }

      predicateExpression = null;
      var channelFilters = parameters.AdditionalParameters.ContainsKey(SearchFiltersParams.Channel_Param)
          ? (parameters.AdditionalParameters[SearchFiltersParams.Channel_Param] as List<SearchItem>)
          : null;
      if (channelFilters != null && channelFilters.Count != 0)
      {
        foreach (SearchItem channelItem in channelFilters)
        {
          Guid channelId = Guid.Parse(channelItem.ItemId);
          if (predicateExpression == null)
            predicateExpression = c => c.InteractionsCache()
                .InteractionCaches.Any(ic => ic.ChannelId == channelId);
          else
            predicateExpression = predicateExpression.Or(c => c.InteractionsCache()
                .InteractionCaches.Any(ic => ic.ChannelId == channelId));
        }
        query = query.Where(predicateExpression);
      }

      predicateExpression = null;
      var outcomeFilters = parameters.AdditionalParameters.ContainsKey(SearchFiltersParams.Outcome_Param)
          ? (parameters.AdditionalParameters[SearchFiltersParams.Outcome_Param] as List<SearchItem>)
          : null;
      if (outcomeFilters != null && outcomeFilters.Count != 0)
      {
        foreach (SearchItem outcomeItem in outcomeFilters)
        {
          Guid outcomeDefinitionId = Guid.Parse(outcomeItem.ItemId);
          if (predicateExpression == null)
            predicateExpression = c => c.InteractionsCache()
                .InteractionCaches.Any(ic => ic.Outcomes.Any(o => o.DefinitionId == outcomeDefinitionId));
          else
            predicateExpression = predicateExpression.Or(c => c.InteractionsCache()
                .InteractionCaches.Any(ic => ic.Outcomes.Any(o => o.DefinitionId == outcomeDefinitionId)));
        }
        query = query.Where(predicateExpression);
      }

      predicateExpression = null;
      var goalFilters = parameters.AdditionalParameters.ContainsKey(SearchFiltersParams.Goal_Param)
          ? (parameters.AdditionalParameters[SearchFiltersParams.Goal_Param] as List<SearchItem>)
          : null;
      if (goalFilters != null && goalFilters.Count != 0)
      {
        foreach (SearchItem goalItem in goalFilters)
        {
          Guid goalDefinitionId = Guid.Parse(goalItem.ItemId);
          if (predicateExpression == null)
            predicateExpression = c => c.InteractionsCache()
                .InteractionCaches.Any(ic => ic.Goals.Any(g => g.DefinitionId == goalDefinitionId));
          else
            predicateExpression = predicateExpression.Or(c => c.InteractionsCache()
                .InteractionCaches.Any(ic => ic.Goals.Any(g => g.DefinitionId == goalDefinitionId)));
        }
        query = query.Where(predicateExpression);
      }

      return GetResult(query, facets, interactionExpandOptions, parameters.PageSize);
    }

    private Task<IAsyncEntityBatchEnumerator<Contact>> GetResult(IAsyncQueryable<Contact> query, string[] facets, RelatedInteractionsExpandOptions interactionExpandOptions, int pageSize)
    {
      var batchEnumerator = query
          .WithExpandOptions(new ContactExpandOptions(facets)
          {
            Interactions = interactionExpandOptions
          }).GetBatchEnumerator(Bookmark, pageSize);

      return batchEnumerator;
    }

    private static IContactSearchResult BuildBaseResult(Contact contact, PersonalInformation personalInformation, EmailAddressList emailAddressList, EngagementMeasures enaEngagementMeasures)
    {
      var ident = contact.Identifiers.Any(identifier => identifier.IdentifierType == ContactIdentifierType.Known);
      var identType = ident == true ? ContactIdentificationLevel.Known : ContactIdentificationLevel.Anonymous;
      var contactSearch = new ContactSearchResult
      {
        IdentificationLevel = (int)identType,
        ContactId = contact.Id.GetValueOrDefault(),
        FirstName = personalInformation?.FirstName,
        MiddleName = personalInformation?.MiddleName,
        Surname = personalInformation?.LastName,
        PreferredEmail = emailAddressList?.PreferredEmail?.SmtpAddress,
        JobTitle = personalInformation?.JobTitle,
        Value = (enaEngagementMeasures != null) ? enaEngagementMeasures.TotalValue : 0,
        VisitCount = (enaEngagementMeasures != null) ? enaEngagementMeasures.TotalInteractionCount : 0
      };

      return contactSearch;
    }

    private static void PopulateLatestVisit(Contact contact, Interaction interaction, IpInfo ipInfo, ref IContactSearchResult contactSearch)
    {
      contactSearch.LatestVisitId = interaction.Id.GetValueOrDefault();
      contactSearch.LatestVisitStartDateTime = interaction.StartDateTime;
      contactSearch.LatestVisitEndDateTime = interaction.EndDateTime;
      contactSearch.LatestVisitPageViewCount = interaction.Events.OfType<PageViewEvent>().Count();
      contactSearch.LatestVisitValue = interaction.EngagementValue;
      var engagementMeasure = contact.EngagementMeasures();
      if (engagementMeasure != null)
        contactSearch.ValuePerVisit = Calculator.GetAverageValue(engagementMeasure.TotalValue, engagementMeasure.TotalInteractionCount);

      if (null != ipInfo)
      {
        contactSearch.LatestVisitLocationCityDisplayName = ipInfo.City;
        contactSearch.LatestVisitLocationCountryDisplayName = ipInfo.Country;
        contactSearch.LatestVisitLocationRegionDisplayName = ipInfo.Region;
        contactSearch.LatestVisitLocationId = null;//ToDo:Changed to xConnect-Location hasn’t been replaced yet
      }
    }

    /// <summary>
    /// Apply device filters
    /// </summary>
    /// <param name="filteredInteractions"></param>
    /// <param name="deviceFilters"></param>
    /// <returns></returns>
    private static IEnumerable<Interaction> ApplyDeviceFilter(IEnumerable<Interaction> filteredInteractions, List<SearchItem> deviceFilters)
    {
      if (deviceFilters != null && deviceFilters.Count != 0)
        filteredInteractions = filteredInteractions.Where(i => deviceFilters.Select(d => d.ItemId).Contains(GetDevicetype(i.UserAgent)));

      return filteredInteractions;
    }

    /// <summary>
    /// Get device type name when it is enabled and ready. Or the device would be Unknown.
    /// </summary>
    /// <param name="userAgent"></param>
    /// <returns></returns>
    public static string GetDevicetype(string userAgent)
    {
      //TODO: This logic needs to be more optimized. Or, use Interaction.UserAgentInfo() extension method when it return device type.
      var deviceType = "Unknown";

      if (CES.DeviceDetection.DeviceDetectionManager.IsEnabled == true && CES.DeviceDetection.DeviceDetectionManager.IsReady == true)
        deviceType = CES.DeviceDetection.DeviceDetectionManager.GetDeviceInformation(userAgent).DeviceType.ToString();
      else if (CES.DeviceDetection.DeviceDetectionManager.IsEnabled == true && CES.DeviceDetection.DeviceDetectionManager.IsReady == false)
      {
        int waitCounter = 0;
        TimeSpan timeout = new TimeSpan(3000);
        while (CES.DeviceDetection.DeviceDetectionManager.IsReady)
        {
          if (CES.DeviceDetection.DeviceDetectionManager.IsReady)
          {
            deviceType = CES.DeviceDetection.DeviceDetectionManager.GetDeviceInformation(userAgent).DeviceType.ToString();
            break;
          }
          else if (waitCounter == 4)
            break;
          CES.DeviceDetection.DeviceDetectionManager.CheckInitialization(timeout);
          waitCounter++;
        }
      }

      return deviceType;
    }
    #endregion
  }
}
