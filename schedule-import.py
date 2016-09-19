import pandas
import csv
import time

input_csv = 'input_file.csv'
bio_csv = 'bio-export.csv'

output_file = 'output-{}.csv'.format(time.strftime("%Y%m%d-%H%M%S"))


"""
    Format is as follows to map input file fields to output fields:

    ('Output Field Label', ('Input Field Label 1',
                            'Input Field Label 2',
                            ....
                            'delimiter',
                            ),
    )

    The final member of the tuple specifies what should be used as between
    when concatenating the data from more than one field, since some require
    a comma but others want a space, for example.

    For speaker data, we have up to 12 speakers listed in the report from AI,
    so it's easiest to separate those as a generic set with placeholder X.
    Then, when iterating over these fields, simply track the index of the
    loop and replace the X in the string with that number.

    For fields with nothing but a direct 1-to-1 correspondence, the dict
    value is just a tuple with one member. This allows testing for this
    need to concatenate using a simple length check.

    Use None value for fields not being used.
"""
mappings = [
    ('Unique ID', ('Event ID',)),
    ('Name', ('Event Title',)),
    ('Description', ('Description',)),
    ('Activity Code', None),
    ('Track', ('Primary Topic Area',)),
    ('Tags (comma-separated)', ('Type',
                                'Session Level',
                                'Intended Audience_Administrators',
                                'Intended Audience_Businesses and Community',
                                'Intended Audience_Faculty',
                                'Intended Audience_Graduate Students or Above',
                                'Intended Audience_Other Staff',
                                'Intended Audience_Sustainability Staff',
                                'Intended Audience_Undergraduate Students',
                                ', ',
                                )),
    ('Start Time', ('Starts OnDate', 'Starts OnTime', ' ',)),
    ('End Time', ('Ends OnDate', 'Ends OnTime', ' ',)),
    ('Location Name', ('Room', 'room ',)),
    ('Parent Activity Unique ID', None),
    ('Group List (comma-separated)', None),
    ('Live Q&A Enabled', None),
]
speaker_mappings = [
    ('Speaker X Display Name', ('First Name_X', 'Last Name_X', ' ',)),
    ('Speaker X First Name', ('First Name_X',)),
    ('Speaker X Last Name', ('Last Name_X',)),
    ('Speaker X Role', ('role_X',)),
    ('Speaker X Title', ('Position/Title_X',)),
    ('Speaker X Bio', ('Bio',)),
    ('Speaker X Email One', ('Primary Email',)),
    ('Speaker X Email Two', None),
    ('Speaker X Organization Name', ('Affiliation_X',))
]


# This function takes the two given csv files and uses pandas to read them
# into data structures that we can work with.
def get_reports(input_file, bio_file):
    # data = np.genfromtxt(input_file, delimiter=',', names=True, dtype=None, usecols=np.arange(0, 120))
    data = pandas.io.parsers.read_csv(input_file, sep=',', header=0)
    # bio_data = np.genfromtxt(bio_file, delimiter=',', names=True, dtype=None, usecols=np.arange(0, 5))
    bio_data = pandas.io.parsers.read_csv(bio_file, sep=',', header=0)
    return data, bio_data


# This function takes the transformed data and writes it, properly formatted
# to the export file csv.
def output_report(data, output_file):
    with open(output_file, "wb") as f:
        writer = csv.writer(f)
        writer.writerows(data)


# This function takes the report (a pandas data structure) and iterates
# over both it and the mappings dictionary to construct the values for
# the fields specified in the latter.
def map_report_data(report, bio_report):
    transformed_data = []
    row = []

    # First just pack all of the Field Labels into the first row
    for field in mappings:
        row.append(field[0])
    for x in range(1, 12):
        for field in speaker_mappings:
            row.append(field[0].replace('X', str(x)))
    transformed_data.append(row)

    # Loop over the report dataframe to generate one line per event
    for index, line in report.iterrows():
        # Initialize blank row to pack values into
        data_row = []
        # Loop over keys in mappings dictionary
        for field in mappings:
            # Ignore fields we aren't mapping to
            if field[1] is not None:
                # If there's only one field being mapped, that's simple
                if len(field[1]) <= 1:
                    # Just add the value of that field to the row
                    data_row.append(line[field[1][0]])

                # Otherwise, we need to enumerate the tuple of fields
                else:
                    concat_value = ''
                    for i, subfield in enumerate(field[1]):
                        # We don't want to include the final item, which
                        # specifies the character used to concatenate fields
                        if (i <= len(field[1])-2) and type(line[subfield]) == str:
                            # Add next value and a concatenation character
                            concat_value += line[subfield]
                            concat_value += field[1][-1]
                    # Get rid of any trailing concatenation characters
                    if concat_value.endswith(field[1][-1]):
                        concat_value = concat_value[:-len(field[1][-1])]

                    # Pack it up and add it as the next entry in this row
                    data_row.append(concat_value)

            # If we're not mapping anything, just insert "None"
            else:
                data_row.append(None)

        # We have speakers 1-12
        for x in range(1, 13):
            # For each speaker, loop over the mappings fields
            for field in speaker_mappings:
                # Ignore fields we aren't mapping to
                if field[1] is not None:
                    # If there's only one field being mapped, that's simple
                    if len(field[1]) <= 1:
                        # Just add the value of that field to the row
                        # For two fields, we get that from the bio report
                        key = field[1][0].replace('X', str(x))
                        if field[1][0] != 'Bio' and field[1][0] != 'Primary Email':
                            # Make sure it's not a NaN value
                            if type(report.loc[index, key]) == str:
                                data_row.append(report.loc[index, key])
                            else:
                                data_row.append(None)

                        # Get those bio report fields
                        else:
                            # Find the bio row that matches the speaker ID in the report row
                            speaker_id = report.loc[index, 'Speaker ID_{}'.format(str(x))]
                            row = bio_report.loc[bio_report['ID'] == speaker_id]
                            # Check if we found something
                            if not row.empty:
                                row = row.iloc[0]
                                # Make sure it's not a NaN value
                                if type(row.loc[key]) == str:
                                    data_row.append(row.loc[key])
                                else:
                                    data_row.append(None)
                            # If we got zero rows back, we have no extra speaker data,
                            # just return None.
                            else:
                                data_row.append(None)

                    # Otherwise, we need to enumerate the tuple of fields
                    else:
                        concat_value = ''
                        for i, subfield in enumerate(field[1]):
                            # We don't want to include the final item, which
                            # specifies the character used to concatenate fields
                            label = subfield.replace('X', str(x))
                            if (i <= len(field[1])-2) and type(line[label]) == str:
                                # Add next value and a concatenation character
                                concat_value += line[label]
                                concat_value += field[1][-1]
                        # Get rid of any trailing concatenation characters
                        if concat_value.endswith(field[1][-1]):
                            concat_value = concat_value[:-len(field[1][-1])]

                        # Pack it up and add it as the next entry in this row
                        data_row.append(concat_value)
                # If we're not mapping anything, just insert "None"
                else:
                    data_row.append(None)

        # Finally, pack it up and add it to the final transformed_data array
        transformed_data.append(data_row)
    return transformed_data


if __name__ == '__main__':
    report, bio_report = get_reports(input_csv, bio_csv)
    processed_data = map_report_data(report, bio_report)
    output_report(processed_data, output_file)
