import nose.tools as n

import links

def test_socrata_225404():
    view = {
      'tableId': 225404,
      'viewType': 'href',
      'metadata': {"href": "http://www.data.gov/details/4006", "accessPoints": {"xlsSize": "0.042 MB", "xls": "http://www.nrc.gov/reactors/operating/oversight/fire-event-data.xls"}, "custom_fields": {"Contributing Agency Information": {"Citation": "http://www.nrc.gov/reactors/operating/oversight/fire-event-data.xls", "Agency Data Series Page": "http://www.nrc.gov/reactors/operating/oversight.html", "Agency Program Page": "http://www.nrc.gov/reactors/operating/oversight.html"}, "Dataset Coverage": {"Unit of Analysis": "Fire Event Data from Licensee Event Reports", "Geographic Coverage": "United States"}, "Dataset Summary": {"High Value Dataset": "Y", "Suggested by Public": "N", "Agency": "Nuclear Regulatory Commission", "Frequency": "Semi-Annual", "Date Updated": "09/30/2010", "Date Released": "06/01/2010", "Time Period": "1/1/1990-8/31/2010"}, "Dataset Information": {"Data.gov Data Category Type": "Raw Data Catalog", "Specialized Data Category Designation": "Administrative", "Extended Type": "Raw Data", "Unique ID": "4006"}, "Data Description": {"Collection Mode": "Person/Computer", "Data Dictionary": "http://www.nrc.gov/reactors/operating/oversight/fire-events-datadictionary.xls", "Data Collection Instrument": "N/A"}, "Additional Dataset Documentation": {"Technical Documentation": "N/A"}, "Data Quality": {"Applicable Information Quality Guideline Designation": "U.S. Nuclear Regulatory Commission", "Privacy and Confidentiality": "Yes", "Data Quality Certification": "Yes"}}},
    }
    expected = {
        'is_link': True,
        'url': "http://www.nrc.gov/reactors/operating/oversight/fire-event-data.xls",
        'software': 'socrata',
        'identifier': 225404,
    }
    n.assert_dict_equal(links.socrata(view), expected)
