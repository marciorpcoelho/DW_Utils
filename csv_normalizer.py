import sys
from os import listdir
import re
import pyodbc
import time


regex_dict = {
    'new_lines': r'(\r\n|\r|\n)',
    'delimiter': r';',
    'matches': r'"(\r\n|\r|\n)$',  # Encontra os CRLF no final de cada string, precedidos de aspas
    'plicas_middle': r'(?:[^;"])("{1,})(?:[^;"\r|;"\n])',  # Encontra as plicas que estão no meio do campo
    'plicas_left': r'[;]("{2,})',  # Encontra as plicas à esquerda do campo
    'plicas_right': r'("{2,})[;|\r|\n|\r\n]',  # Encontra as plicas à direita do campo
}


def main(source, target):
    start = time.time()

    files_name, in_files_name_and_path, out_files_name_and_path = get_all_csv_files(source, target)

    for file_name, in_file_name_path, out_file_name_and_path in zip(files_name, in_files_name_and_path, out_files_name_and_path):
        # normalizer_alt(file_name, in_file_name_path, out_file_name_and_path)
        step_1_count, step_2_lines_input, step_2_lines_output, normalize_time = normalizer(file_name, in_file_name_path, out_file_name_and_path)
        log_creation(step_1_count, step_2_lines_input, step_2_lines_output, file_name, target, normalize_time)

    print('Total Elapsed Time: {:.2f}'.format(time.time() - start))


def log_creation(step_1_count, step_2_lines_input, step_2_lines_output, file_name, target, timer):

    log_file_name = file_name + '_log'
    time_tag = time.strftime("%Y-%m-%d_%H.%M.%S")
    step_2_corrections_count = len(step_2_lines_output)

    # Local File
    with open(target + time_tag + '_' + log_file_name + '.txt', 'w') as log_file:
        log_file.write('Log record for file {}:\n'.format(file_name))
        log_file.write('Step 1 - Number of lines with wrongfully placed CRLF: {}\n'.format(step_1_count))
        log_file.write('Step 2 - Number of line delimiters " corrected: {}\n'.format(step_2_corrections_count))
        log_file.write('Normalization duration: {:.3f} seconds\n'.format(timer))
    log_file.close()

    with open(target + time_tag + '_' + log_file_name + '_step_2.txt', 'w') as log_file_step_2:
        log_file_step_2.write('Log record for file {}:\n'.format(file_name))
        [(log_file_step_2.write('Input Line:  {}'.format(step_2_input)), log_file_step_2.write('Output Line: {}\n'.format(step_2_output))) for step_2_input, step_2_output in zip(step_2_lines_input, step_2_lines_output)]
    log_file_step_2.close()

    # SQL DW
    # connection = pyodbc.connect('Driver={SQL Server};'
    #                             'Server=SCRCGAISQLD1\\DEV01;'
    #                             'Database=BI_MLG;'
    #                             'Trusted_Connection=yes;')
    #
    # cursor = connection.cursor()
    # cursor.execute(''' INSERT INTO BI_MLG.dbo.LOG_Information VALUES ('{}', {}, '{}', '{}', {})'''.format('{} - Log record for file {}:\n'.format(file_name, file_name), 1, '10:10:22', '20190910', 9999))
    # cursor.execute(''' INSERT INTO BI_MLG.dbo.LOG_Information VALUES ('{}', {}, '{}', '{}', {})'''.format('{} - Step 1 - Number of lines with wrongfully placed CRLF: {}\n'.format(file_name, step_1_count), 2, '10:10:22', '20190910', 9999))
    # cursor.execute(''' INSERT INTO BI_MLG.dbo.LOG_Information VALUES ('{}', {}, '{}', '{}', {})'''.format('{} - Step 2 - Number of line delimiters " corrected: {}\n'.format(file_name, step_2_corrections_count), 3, '10:10:22', '20190910', 9999))
    # cursor.execute(''' INSERT INTO BI_MLG.dbo.LOG_Information VALUES ('{}', {}, '{}', '{}', {})'''.format('{} - Normalization duration: {:.3f} seconds\n'.format(file_name, timer), 4, '10:10:22', '20190910', 9999))

    # These lines are just for an idea I had which is to use Bulk Insert. The limitation is that bulk insert needs a csv?
    # step_2 = []
    # [(step_2.append(x), step_2.append(y)) for x, y in zip(step_2_lines_input, step_2_lines_output)]

    # for line_id in range(step_2_corrections_count):
    #     cursor.execute(''' INSERT INTO BI_MLG.dbo.LOG_Information VALUES ('{}', {}, '{}', '{}', {})'''.format('{} - Input Line  {}:'.format(file_name, step_2_lines_input[line_id]), 1, '10:10:50', '20190910', 9999))
    #     cursor.execute(''' INSERT INTO BI_MLG.dbo.LOG_Information VALUES ('{}', {}, '{}', '{}', {})'''.format('{} - Output Line {}:'.format(file_name, step_2_lines_output[line_id]), 1, '10:10:50', '20190910', 9999))

    # connection.commit()
    # connection.close()

    return


def multireplace(string, replacements, step_2_lines_input, step_2_lines_output):
    number_of_corrections = 0

    substrs = sorted(replacements, key=len, reverse=True)

    # Create a big OR regex that matches any of the substrings to replace
    regexp = re.compile('|'.join(substrs))

    iterator = re.finditer(regexp, string)
    sel_iterator = [x for x in iterator if len(x.group()) % 2]

    for x in sel_iterator:
        if string[x.end() + number_of_corrections] == ';':  # Campos vazios
            continue
        else:
            step_2_lines_input.append(string[1:-1])
            new_group = re.sub('"', '""', x.group(), count=1)  # Replaces " by "", effectively adding one ", only once
            string = ''.join((string[:x.start() + number_of_corrections], new_group, string[x.end() + number_of_corrections:]))
            step_2_lines_output.append(string[1:-1])
            number_of_corrections += 1

    return string, step_2_lines_input, step_2_lines_output


def get_all_csv_files(source, target):

    # onlyfiles = [f[:-4] for f in listdir(source) if f.endswith('.csv')]
    onlyfiles = ['BIFD_SOHeaders_Comments']
    # onlyfiles = ['test.csv', 'test_2.csv']  # For testing Purposes

    if onlyfiles:
        source_files = [source + x + '.csv' for x in onlyfiles]
        target_files = [target + x + '.csv' for x in onlyfiles]
        return onlyfiles, source_files, target_files
    else:
        raise FileNotFoundError('Pasta de origem {} sem ficheiros csv.'.format(source))


def normalizer_alt(file_name, in_file_name, out_file_name):  # Used for the replace of a single symbol and some particular cases
    reg = re.compile(r'"')

    with open(in_file_name) as in_file:
        with open(out_file_name, 'w') as out_file:
            for line in in_file:
                # print(line)
                if line == '''"4";"16";"2018/N2-1/51618";"20180305";"20092";"APOS A REPARAÇÃO OR 51584 VIAT";"20180305"\n''':
                    print('line found!', line)
                # count = len(re.findall(reg, line))
                # if count:
                #     # iterator = re.finditer(reg, line)
                #     line = re.sub(reg, '|', line)

                out_file.write(line)

        out_file.close()
    in_file.close()


def normalizer(file_name, in_file_name, out_file_name):
    print('Normalizing file {}...'.format(file_name))

    start = time.time()
    reg = re.compile(regex_dict['new_lines'])
    reg_delimiter = re.compile(regex_dict['delimiter'])
    run_once, step_1_count, step_2_lines_input, step_2_lines_output = 0, 0, [], []

    replacements = {
        regex_dict['plicas_middle']: 'middle',
        regex_dict['plicas_left']: 'left',
        regex_dict['plicas_right']: 'right',
    }

    current_delimiter_count, delimiter_count = 0, 0
    prev_line, total_line, line = '', '', ''
    with open(in_file_name) as in_file:
        with open(out_file_name, 'w') as out_file:

            for line in in_file:
                # print('prev_line: {} \nline: {}'.format(prev_line, line))
                if not run_once:
                    delimiter_count = len(re.findall(reg_delimiter, line))
                    # print('delimiter_count', delimiter_count)
                run_once = 1

                current_delimiter_count = len(re.findall(reg_delimiter, line))
                # print('current_delimiter_count', current_delimiter_count)

                if current_delimiter_count == delimiter_count:
                    # print('all good!')
                    prev_line = ''
                    total_line = line

                    total_line = ''.join((';', total_line, ';'))

                    total_line, step_2_lines_input, step_2_lines_output = multireplace(total_line, replacements, step_2_lines_input, step_2_lines_output)

                    out_file.write(total_line[1:-1])
                    # print(total_line[1:-1])

                    # print('all good \n')
                elif current_delimiter_count < delimiter_count:
                    # print('inside')
                    # print('prev_line', prev_line)
                    total_line = ''.join((prev_line, line))
                    # print('total line:', total_line)

                    current_delimiter_count = len(re.findall(reg_delimiter, total_line))
                    # print('current_delimiter_count', current_delimiter_count)

                    if current_delimiter_count < delimiter_count:
                        # print('inside 2')
                        total_line = re.sub(reg, '', total_line)
                        step_1_count += 1
                        # print('am i here? total line is {}'.format(total_line))
                        prev_line = total_line
                        # print(prev_line)
                    else:
                        # print('is line correct?')
                        # print('total line is {} and current_delimiter_count is {}'.format(total_line, len(re.findall(reg_delimiter, total_line))))
                        # total_line += '\r\n'
                        # print('check here', total_line)

                        total_line = ''.join((';', total_line, ';'))

                        total_line, step_2_lines_input, step_2_lines_output = multireplace(total_line, replacements, step_2_lines_input, step_2_lines_output)

                        out_file.write(total_line[1:-1])
                        prev_line = ''
                        # print(total_line[1:-1])

        out_file.close()
    in_file.close()

    time_to_normalize = time.time() - start
    print('File {} normalized. Elapsed Time: {:.2f}. \n'.format(file_name, time_to_normalize))
    return step_1_count, step_2_lines_input, step_2_lines_output, time_to_normalize


if __name__ == '__main__':
    # source_folder = 'C:\\Users\\mrpc\\Documents\\test_files\\'
    # target_folder = 'C:\\Users\\mrpc\\Documents\\test_files\\files_treated\\'
    source_folder = sys.argv[1]
    target_folder = sys.argv[2]
    main(source_folder, target_folder)

