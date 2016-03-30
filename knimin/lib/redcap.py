from redcap import Project, RedcapError

from knimin import config, db
from .constants import ebi_remove

ag_redcap = Project(config.redcap_url, config.redcap_api_key)


def pulldown_redcap(barcodes, third_party=None):
    """Wrapper for all pulldown logic, giving the formatted metadata

    Parameters
    ----------
    barcodes : list of str
        What barcodes to do pulldown on
    third_party : list of str
        What third party surveys to also attach. Default None

    Returns : dict of str
        formatted tab delimited metadata files, keyed to survey type
        (human, animal, etc)
    """
    surveys = db.get_survey_types()
    records = db.get_records_for_barcodes(barcodes)
    formatted = {}
    for survey in surveys:
        raw_data = _batch_grab(surveys[survey], records)
        if survey == 'Human':
            formatted[survey] = _format_human(raw_data).to_csv(sep='\t')
        elif survey == 'Animal':
            formatted[survey] = _format_animal(raw_data).to_csv(sep='\t')
        else:
            formatted[survey] = raw_data.to_csv(sep='\t')
    return formatted


def _batch_grab(instruments, records, full=False, batch_size=100):
    """Chunks redcap calls so we don't overload the server with large requests

    Parameters
    ----------
    instruments : list of str
        What instruments to export
    records : list of str
        What records to export
    full : bool, optional
        Whether to do full PHI pulldown or not. Default False (no PHI)
    batch_size : int, optional
        Number of records to export at a time. Default 100

    Returns
    -------
    pandas DataFrame
        The full survey information for the records
    """
    # Adapted from https://pycap.readthedocs.org/en/latest/deep.html
    def chunks(l, n):
        """Yield successive n-sized chunks from list l"""
        for i in range(0, len(l), n):
            yield l[i:i+n]

    record_list = ag_redcap.export_records(fields=[ag_redcap.def_field])
    records = [r[ag_redcap.def_field] for r in record_list]
    try:
        response = []
        for record_chunk in chunks(records, batch_size):
            chunked_response = ag_redcap.export_records(
                records=record_chunk, forms=instruments, format='df',
                export_survey_fields=True)
            response.append(chunked_response)
    except RedcapError:
        msg = "Chunked export failed for batch_size={:d}".format(batch_size)
        raise ValueError(msg)

    # Combine the pandas dataframes as a single one optionally removing PHI
    full_df = response[0]
    for chunk in response[1:]:
        full_df.append(chunk)
    if not full:
        full_df = full_df[[x for x in full_df.columns if x not in ebi_remove]]
    return full_df


def _format_human(data):
    """Formats the redcap data export to a tab delimited file for human survey

    Parameters
    ----------
    data : pandas DataFrame
        Raw redcap export

    Returns
    -------
    str
        Tab delimited metadata for human survey
    """


def _format_animal(data):
    """Formats the redcap data export to a tab delimited file for animal survey

    Parameters
    ----------
    data : pandas DataFrame
        Raw redcap export

    Returns
    -------
    str
        Tab delimited metadata for animal survey
    """
