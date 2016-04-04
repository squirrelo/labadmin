from redcap import Project, RedcapError
import pandas as pd
import numpy as np
from datetime import time, datetime

from util import (categorize_age, categorize_etoh, categorize_bmi, correct_age,
                  correct_bmi)
from constants import (md_lookup, month_str_lookup,
                       regions_by_state, blanks_values, season_lookup,
                       ebi_remove, redcap_remove)
from knimin import config, db

ag_redcap = Project(config.redcap_url, config.redcap_api_key)


def pulldown(barcodes, blanks=None, external=None, full=False):
    """Wrapper for all pulldown logic, giving the formatted metadata

    Parameters
    ----------
    barcodes : list of str
        What barcodes to do pulldown on
    blanks : list of str, optional
            Names for the blanks to add. Default None
            Blanks added to Human survey
    external : list of str, optional
        What external surveys to also attach. Default None
    full : bool, optional
        Whether to get full PHI containing info or not. Default False (no PHI)

    Returns
    -------
    dict of str
        Formatted tab delimited metadata files, keyed to survey type
        (Human, Animal, etc)
    failures : dict
            Barcodes unable to pull metadata down, in the form
            {barcode: reason, ...}

    """
    # Survey types can cover multiple instruments for multiple languages
    # so make sure we get all the languages covered
    surveys = db.get_survey_types()
    records = db.get_records_for_barcodes(barcodes)
    backbone = pd.DataFrame(db.get_barcode_surveys(barcodes), dtype=str,
                            columns=['SURVEY_ID', 'BARCODE'])
    backbone.set_index('SURVEY_ID', drop=False, inplace=True)

    formatted = {}
    for survey in surveys:
        raw_data = _batch_grab(surveys[survey], records)
        if len(raw_data.index) == 0:
            continue
        if survey == 'Human':
            df = _format_human(raw_data, backbone, external, full)
            # Add blanks if they are passed
            if blanks is not None:
                blanks_df = pd.DataFrame(
                    index=blanks, columns=df.columns)
                for col in blanks_df.columns:
                    blanks_df[col] = [blanks_values[col]] * len(blanks)
                df = df.append(blanks_df)
        elif survey == 'Animal':
            df = _format_animal(raw_data, backbone)
        else:
            df = _format_basic(raw_data, backbone)

        formatted[survey] = df.to_csv(sep='\t')

    failures = set(barcodes) - set(df.index)
    return formatted, db._explain_pulldown_failures(failures)


def _batch_grab(instruments, records, batch_size=100):
    """Chunks redcap calls so we don't overload the server with large requests

    Parameters
    ----------
    instruments : list of str
        What instruments to export
    records : list of str
        What records to export
    batch_size : int, optional
        Number of records to export at a time. Default 100

    Returns
    -------
    pandas DataFrame
        The full survey information for the records

    Raises
    ------
    RuntimeError
        Batch size too large for data
    """
    # Adapted from https://pycap.readthedocs.org/en/latest/deep.html
    def chunks(l, n):
        """Yield successive n-sized chunks from list l"""
        for i in range(0, len(l), n):
            yield l[i:i+n]

    response = []
    try:
        # Record IDs must be strings for this library, so explicit convert
        # since stored as longs
        for record_chunk in chunks(map(str, records), batch_size):
            chunked_response = ag_redcap.export_records(
                records=record_chunk, forms=instruments, format='df',
                export_survey_fields=True, raw_or_label='label',
                df_kwargs={'dtype': 'str'}, export_checkbox_labels=True)
            response.append(chunked_response)
    except RedcapError:
        msg = "Batched export failed for batch_size={:d}".format(batch_size)
        raise RuntimeError(msg)

    # Combine the pandas dataframes as a single one optionally removing PHI
    full_df = response[0]
    for chunk in response[1:]:
        full_df.append(chunk)
    full_df.columns = [c.upper() for c in full_df.columns]
    full_df.set_index('SURVEY_ID', inplace=True)
    full_df.fillna('Unspecified', inplace=True)
    return full_df


def _geolocate(data, full):
    geolocated = {}
    zip_lookup = db.get_zipcodes(full)
    country_lookup = db.get_countries()
    for index, row in data.iterrows():
        zipcode = unicode(row['ZIP_CODE']).upper()
        country = row['COUNTRY']

        geolocated[index] = {}
        try:
            geolocated[index]['COUNTRY'] = country_lookup[country]
        except KeyError:
            geolocated[index]['COUNTRY'] = country

        try:
            geolocated[index]['LATITUDE'] = zip_lookup[zipcode][country][0]
            geolocated[index]['LONGITUDE'] = zip_lookup[zipcode][country][1]
            geolocated[index]['ELEVATION'] = zip_lookup[zipcode][country][2]
            geolocated[index]['STATE'] = zip_lookup[zipcode][country][3]
        except KeyError:
            # geocode unknown zip/country combo and add to
            # zipcode table & lookup dict
            info = db.get_geocode_zipcode(zipcode, country)
            if info.lat is not None:
                if full:
                    geolocated[index]['LATITUDE'] = info.lat
                    geolocated[index]['LONGITUDE'] = info.long
                    geolocated[index]['ELEVATION'] = info.elev
                else:
                    geolocated[index]['LATITUDE'] = round(info.lat, 1)
                    geolocated[index]['LONGITUDE'] = round(info.long, 1)
                    geolocated[index]['ELEVATION'] = round(info.elev, 1)
                geolocated[index]['STATE'] = info.state
                geolocated[index]['COUNTRY'] = country_lookup[
                    info.country]
                # Store in dict so we don't geocode again
                zip_lookup[zipcode][country] = (
                    geolocated[index]['LATITUDE'],
                    geolocated[index]['LONGITUDE'],
                    geolocated[index]['ELEVATION'],
                    geolocated[index]['STATE'])
            else:
                geolocated[index]['LATITUDE'] = 'Unspecified'
                geolocated[index]['LONGITUDE'] = 'Unspecified'
                geolocated[index]['ELEVATION'] = 'Unspecified'
                geolocated[index]['STATE'] = 'Unspecified'
                geolocated[index]['COUNTRY'] = 'Unspecified'
                # Store in dict so we don't geocode again
                zip_lookup[zipcode][country] = (
                    'Unspecified', 'Unspecified', 'Unspecified',
                    'Unspecified')
        try:
            state = geolocated[index]['STATE']
            data['CENSUS_REGION'] = regions_by_state[state]['Census_1']
            data['ECONOMIC_REGION'] = regions_by_state[state]['Economic']
        except KeyError:
            data['CENSUS_REGION'] = 'Unspecified'
            data['ECONOMIC_REGION'] = 'Unspecified'

    # Combine geolocation data with existing dataframe
    del data['COUNTRY']
    geo_df = pd.DataFrame.from_dict(geolocated, orient='index')
    return data.join(geo_df)


def _format_human(data, backbone, external=None, full=False):
    """Formats the redcap data export to a complete dataframe for human survey

    Parameters
    ----------
    data : pandas DataFrame
        Raw redcap export
    backbone : pandas DataFrame
        barcodes list, indexed on survey_id
    external : list of str
        External surveys to pulldown as well. Default None
    full : bool, optional
        Whether to do a full PHI containing pulldown or not.
        Default False (no PHI)

    Returns
    -------
    pandas DataFrame
        Complete metadata for human survey
    """
    # Add location information
    data = _geolocate(data, full)

    # Invariant information
    num_rows = len(data.index)
    data['HOST_TAXID'] = [9606] * num_rows
    data['TITLE'] = ['American Gut Project'] * num_rows
    data['ALTITUDE'] = [0] * num_rows
    data['ASSIGNED_FROM_GEO'] = ['Yes'] * num_rows
    data['ENV_BIOME'] = ['ENVO:dense settlement biome'] * num_rows
    data['ENV_FEATURE'] = ['ENVO:human-associated habitat'] * num_rows
    data['DEPTH'] = [0] * num_rows
    data['DNA_EXTRACTED'] = ['Yes'] * num_rows
    data['HAS_PHYSICAL_SPECIMEN'] = ['Yes'] * num_rows
    data['PHYSICAL_SPECIMEN_REMAINING'] = ['Yes'] * num_rows
    data['PHYSICAL_SPECIMEN_LOCATION'] = ['UCSDMI'] * num_rows
    data['REQUIRED_SAMPLE_INFO_STATUS'] = ['completed'] * num_rows
    data['HOST_COMMON_NAME'] = ['human'] * num_rows
    data['PUBLIC'] = ['Yes'] * num_rows
    data['HOST_SUBJECT_ID'] = data['RECORD_ID']

    # Add categorization and correction columns
    data['ALCOHOL_CONSUMPTION'] = data['ALCOHOL_FREQUENCY'].apply(
        categorize_etoh)
    data['BMI_CORRECTED'] = data['BMI'].apply(correct_bmi)
    data['BMI_CAT'] = data['BMI_CORRECTED'].apply(categorize_bmi)
    data['AGE_CORRECTED'] = np.vectorize(correct_age, otypes=[str])(
        data['AGE_YEARS'], data['HEIGHT_CM'], data['WEIGHT_KG'],
        data['ALCOHOL_CONSUMPTION'])
    data['AGE_CAT'] = data['AGE_CORRECTED'].apply(categorize_age)

    # Add subset columns (All boolean)
    data['SUBSET_AGE'] = data['AGE_YEARS'].apply(
        lambda x: x != 'Unspecified' and 19 < x < 70)
    data['SUBSET_DIABETES'] = data['DIABETES'].apply(
        lambda x: x == 'I do not have this condition')
    data['SUBSET_IBD'] = data['IBD'].apply(
        lambda x: x == 'I do not have this condition')
    data['SUBSET_ANTIBIOTIC_HISTORY'] = data['ANTIBIOTIC_HISTORY'].apply(
        lambda x: x == 'I have not taken antibiotics in the past year.')
    data['SUBSET_BMI'] = data['BMI'].apply(
        lambda x: x != 'Unspecified' and 18.5 <= x < 30)
    data['SUBSET_HEALTHY'] = np.vectorize(
        lambda *args: all(args), otypes=[str])(
        data['SUBSET_AGE'], data['SUBSET_DIABETES'], data['SUBSET_IBD'],
        data['SUBSET_ANTIBIOTIC_HISTORY'], data['SUBSET_BMI'])

    # Address birthday issue
    data['BIRTH_MONTH'] = [x.split('-')[1] if x != 'Unspecified' else x
                           for x in data['BIRTH_DATE']]
    data['BIRTH_YEAR'] = [x.split('-')[0] if x != 'Unspecified' else x
                          for x in data['BIRTH_DATE']]
    del data['BIRTH_DATE']

    # Add external surveys, if needed
    if external is not None:
        for survey in external:
            ext = db.get_external_survey(survey, data.index)
            data.join(ext, how='left')

    # Combine the backbone and data to get dataframe keyed to barcodes
    combined = backbone.join(data)
    combined = combined.loc[data.index]
    combined.set_index('BARCODE', inplace=True)
    combined['ANONYMIZED_NAME'] = list(combined.index)

    bc_details = db.get_ag_barcode_details(list(combined.index))
    for barcode, row in combined.iterrows():
        bc_info = bc_details[barcode]
        site = bc_info['site_sampled']

        combined.loc[barcode, 'COLLECTION_SEASON'] = season_lookup[
            bc_info['sample_date'].month]

        combined.loc[barcode, 'COLLECTION_MONTH'] = month_str_lookup.get(
            bc_info['sample_date'].month, 'Unspecified')
        try:
            combined.loc[barcode, 'TAXON_ID'] = md_lookup[site]['TAXON_ID']
        except KeyError:
            raise KeyError("Unknown body site for barcode %s: %s" %
                           (barcode, site))
        except:
            raise

        combined.loc[barcode, 'COMMON_NAME'] = md_lookup[site]['COMMON_NAME']
        combined.loc[barcode, 'COLLECTION_DATE'] = \
            bc_info['sample_date'].strftime('%m/%d/%Y')

        if bc_info['sample_time']:
            combined.loc[barcode, 'COLLECTION_TIME'] = \
                bc_info['sample_time'].strftime('%H:%M')
        else:
            # If no time data, show unspecified and default to midnight
            combined.loc[barcode, 'COLLECTION_TIME'] = 'Unspecified'
            bc_info['sample_time'] = time(0, 0)

        combined.loc[barcode, 'COLLECTION_TIMESTAMP'] = datetime.combine(
            bc_info['sample_date'],
            bc_info['sample_time']).strftime('%m/%d/%Y %H:%M')
        combined.loc[barcode, 'ENV_MATTER'] = md_lookup[site]['ENV_MATTER']
        combined.loc[barcode, 'SCIENTIFIC_NAME'] = \
            md_lookup[site]['SCIENTIFIC_NAME']
        combined.loc[barcode, 'SAMPLE_TYPE'] = md_lookup[site]['SAMPLE_TYPE']
        combined.loc[barcode, 'BODY_HABITAT'] = md_lookup[site]['BODY_HABITAT']
        combined.loc[barcode, 'BODY_SITE'] = md_lookup[site]['BODY_SITE']
        combined.loc[barcode, 'BODY_PRODUCT'] = md_lookup[site]['BODY_PRODUCT']
        combined.loc[barcode, 'DESCRIPTION'] = md_lookup[site]['DESCRIPTION']

    combined.drop(redcap_remove, axis=1, inplace=True)
    if not full:
        combined.drop(ebi_remove, axis=1, inplace=True)
    return combined


def _format_animal(data, backbone):
    """Formats the redcap data export to a complete dataframe for animal survey

    Parameters
    ----------
    data : pandas DataFrame
        Raw redcap export
    backbone : pandas DataFrame
        barcodes list, indexed on survey_id

    Returns
    -------
    pandas DataFrame
        Complete metadata for human survey
    """
    # Invariant information
    num_rows = len(data.index)
    # data['HOST_TAXID'] = ????
    data['TITLE'] = ['American Gut Project'] * num_rows
    data['ALTITUDE'] = [0] * num_rows
    data['ASSIGNED_FROM_GEO'] = ['Yes'] * num_rows
    data['ENV_BIOME'] = ['ENVO:dense settlement biome'] * num_rows
    data['ENV_FEATURE'] = ['ENVO:animal-associated habitat'] * num_rows
    data['DEPTH'] = [0] * num_rows
    data['DESCRIPTION'] = ['American Gut Project Animal sample'] * num_rows
    data['DNA_EXTRACTED'] = ['Yes'] * num_rows
    data['HAS_PHYSICAL_SPECIMEN'] = ['Yes'] * num_rows
    data['PHYSICAL_SPECIMEN_REMAINING'] = ['Yes'] * num_rows
    data['PHYSICAL_SPECIMEN_LOCATION'] = ['UCSDMI'] * num_rows
    data['REQUIRED_SAMPLE_INFO_STATUS'] = ['completed'] * num_rows

    # Combine the backbone and data to get dataframe keyed to barcodes
    combined = backbone.join(data)
    combined.set_index('BARCODE', inplace=True)
    combined['ANONYMIZED_NAME'] = list(combined.index)
    return combined


def _format_basic(data, backbone):
    """Formats the redcap data export to a complete dataframe for other survey

    Parameters
    ----------
    data : pandas DataFrame
        Raw redcap export
    backbone : pandas DataFrame
        barcodes list, indexed on survey_id

    Returns
    -------
    pandas DataFrame
        Complete metadata for human survey
    """
    # Invariant information
    num_rows = len(data.index)
    # data['HOST_TAXID'] = ????
    data['TITLE'] = ['American Gut Project'] * num_rows
    data['ALTITUDE'] = [0] * num_rows
    data['ASSIGNED_FROM_GEO'] = ['Yes'] * num_rows
    # data['ENV_BIOME'] = ???
    # data['ENV_FEATURE'] = ???
    data['DEPTH'] = [0] * num_rows
    data['DESCRIPTION'] = ['American Gut Project sample'] * num_rows
    data['DNA_EXTRACTED'] = ['Yes'] * num_rows
    data['HAS_PHYSICAL_SPECIMEN'] = ['Yes'] * num_rows
    data['PHYSICAL_SPECIMEN_REMAINING'] = ['Yes'] * num_rows
    data['PHYSICAL_SPECIMEN_LOCATION'] = ['UCSDMI'] * num_rows
    data['REQUIRED_SAMPLE_INFO_STATUS'] = ['completed'] * num_rows

    # Combine the backbone and data to get dataframe keyed to barcodes
    combined = backbone.join(data, how='inner')
    combined.set_index('BARCODE')
    data['ANONYMIZED_NAME'] = list(combined.index)
    return combined
