############################################################################
# DATA MINING (WQD 7005)
# Online Assessment - Data Cleansing
# Group members : Sivanesan Pillai (WQD 170074)
##############################################################################

import pandas as pd
class DataCleaner:

    def __init__(self, old_separator=',', new_separator='\t'):
        self.old_separator = old_separator
        self.new_separator = new_separator

    def validate_formatting(self, data_header):

        try:
            columns = len(data_header['columns'])
            types = len(data_header['types'])
            positions = len(data_header['positions'])
            ignore_nan = len(data_header['ignore_nan'])

            if columns == 0 or types == 0 or positions == 0 or ignore_nan == 0:
                raise Exception('Empty data header.')
            elif not (columns == types and types == positions and positions == ignore_nan):
                raise Exception('Missing values in data header.')
        except KeyError:
            raise Exception('Invalid or unformatted data header.')

        return True

    def read_data(self, file, data_header):

        data_file = open(file, 'r')
        data_rows = []
        first_size = 0

        for line in data_file:
            line = line.strip().split(self.old_separator)

            if len(line) != first_size:
                if first_size != 0:
                    continue
                first_size = len(line)

            item = {}
            all_ok = True

            for indx, column in enumerate(data_header['columns']):

                try:
                    position = data_header['positions'][indx]
                    column_type = data_header['types'][indx]
                    ignore_nan = data_header['ignore_nan'][indx]

                    if not ignore_nan and (line[position].strip() == '' or line[position] == '0'):
                        all_ok = False
                        break
                    item[column] = column_type(line[position])

                except ValueError:
                    raise Exception(
                        'Invalid data type at column {0}, data {1}, expected type {2}.'.format(column, line[position],
                                                                                               column_type))

            if all_ok:
                data_rows.append(item)

        data_file.close()

        data_cleaned = pd.DataFrame(data_rows)

        return data_cleaned

    def clean(self, file, data_header):

        if self.validate_formatting(data_header):
            data_cleaned = self.read_data(file, data_header)
            data_cleaned.to_csv('cleaned_' + file, sep=self.new_separator, encoding='utf-8', index=False)


if __name__ == '__main__':
    # Data from the city of Chicago
    # https://data.cityofchicago.org/
    file = 'crimes_2018_chicago.csv'

    data_header = {'columns': ['Date', 'Type', 'Latitude', 'Longitude'], \
                   'types': [str, str, float, float], \
                   'positions': [1, 4, 15, 16], \
                   'ignore_nan': [0, 0, 0, 0]}

    dc = DataCleaner()
    dc.clean(file, data_header)