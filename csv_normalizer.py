import sys
import re
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
    # file_name = 'dbs/BIFD_SOHeaders_Comments_Orig'
    # file_name_2 = 'dbs/BIFD_SOActivities_Orig'
    # file_name_3 = 'D:\\BIFD_NLOpenAccountsMov'

    files_name, in_files_name_and_path = get_all_csv_files(source)
    out_files_name_and_path = [target + x for x in files_name]

    start = time.time()

    for file_name, in_file_name_path, out_files_name_and_path in zip(files_name, in_files_name_and_path, out_files_name_and_path):
        print('Normalizing file {}...'.format(file_name))
        start_1 = time.time()
        normalizer(file_name, in_file_name_path, out_files_name_and_path)
        print('File {} normalized. Elapsed Time: {:.2f}'.format(file_name, (time.time() - start_1)))

    print('Total Elapsed Time: {:.2f}'.format(time.time() - start))


def multireplace(string, replacements):
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
            new_group = re.sub('"', '""', x.group(), count=1)  # Replaces " by "", effectively adding one ", only once
            string = ''.join((string[:x.start() + number_of_corrections], new_group, string[x.end() + number_of_corrections:]))
            number_of_corrections += 1

    return string


def get_all_csv_files(source):

    # onlyfiles = [f for f in listdir(source) if f.endswith('.csv')]
    onlyfiles = ['BIFD_NLOpenAccountsMov.csv']
    # onlyfiles = ['test.csv']  # For testing Purposes

    if onlyfiles:
        files_to_treat = [source + x for x in onlyfiles]
        return onlyfiles, files_to_treat
    else:
        raise FileNotFoundError('Pasta de origem {} sem ficheiros csv.'.format(source))


def normalizer(file_name, in_file_name, out_file_name):
    reg = re.compile(regex_dict['new_lines'])
    reg_delimiter = re.compile(regex_dict['delimiter'])
    count = 0

    replacements = {
        regex_dict['plicas_middle']: 'middle',
        regex_dict['plicas_left']: 'left',
        regex_dict['plicas_right']: 'right',
    }

    lines_step_1 = []

    method = 3

    # Method #1
    # for line in fileinput.input(in_file_name, inplace=1):
    #
    #     if in_file_name == 'C:\\Users\\mrpc\\Documents\\mrpc - Pessoal\\PyCharm Projects\\test_files\\BIFD_SOHeaders_Comments_Orig.csv' and '"208";"72";"2018/OT3/922";"20180313";"3727";"21/03 (JR) HABLO CON CLIENTA' in line:
    #         line = ';"208";"72";"2018/OT3/922";"20180313";"3727";"21/03 (JR) HABLO CON CLIENTA, LE COMENTO QUE ES IMPOSIBLE TENERLO PARA HOY COMO ME DIJO ISRAEL, VAMOS A INTENTAR PARA EL VIERNES PERO ESTA COMLICADO, NO SE LO TOMA BIEN, HACE VARIOS DESPRECIOS, LE DIGO QUE ES UN LATERAL COMPLETO Y NO ES COSA DE DOS DIAS Y LA PIEZA SE HA RETRASADO EL PANEL DE PUERTA. DICE QUE HABLARA CON SU MADRE PARA QUE LLAME Y ""CANTE LAS 40"";'
    #     else:
    #         if not re.search(regex_dict['matches'], line):
    #             line = re.sub(reg, '', line)
    #
    #         line = ''.join((';', line, ';'))
    #
    #         line = multireplace(line, replacements)
    #
    #     sys.stdout.write(line[1:-1])

    if method == 2:
        # Method #2
        with open(in_file_name) as in_file:
            with open(out_file_name, 'w') as out_file_name:

                for line in in_file:
                    if file_name == 'BIFD_SOHeaders_Comments.csv' and '"208";"72";"2018/OT3/922";"20180313";"3727";"21/03 (JR) HABLO CON CLIENTA' in line:
                        print('Exception 1 found.')
                        line = ';"208";"72";"2018/OT3/922";"20180313";"3727";"21/03 (JR) HABLO CON CLIENTA, LE COMENTO QUE ES IMPOSIBLE TENERLO PARA HOY COMO ME DIJO ISRAEL, VAMOS A INTENTAR PARA EL VIERNES PERO ESTA COMLICADO, NO SE LO TOMA BIEN, HACE VARIOS DESPRECIOS, LE DIGO QUE ES UN LATERAL COMPLETO Y NO ES COSA DE DOS DIAS Y LA PIEZA SE HA RETRASADO EL PANEL DE PUERTA. DICE QUE HABLARA CON SU MADRE PARA QUE LLAME Y ""CANTE LAS 40"";'

                    elif file_name == 'BIFD_SOHeaders_Comments.csv' and '"2";"24";"2019/AOJ/152";"20190704";"3024";"Agendado dia 12/08/2019 09:00 | Comp.ª Seguros""Logo"' in line:
                        print('Exception 2 found.')
                        line = ';Comp.ª Seguros""Logo"";'

                    elif file_name == 'BIFD_SOHeaders_Comments.csv' and '"2";"24";"2019/AOJ/159";"20190704";"3024";"Comp. Seg. ""Tranquilidade"' in line:
                        print('Exception 3 found.')
                        line = ';"2";"24";"2019/AOJ/159";"20190704";"3024";"Comp. Seg. ""Tranquilidade"";'

                    else:
                        if not re.search(regex_dict['matches'], line):
                            lines_step_1.append(line)
                            line = re.sub(reg, '', line)

                        line = ''.join((';', line, ';'))

                        line = multireplace(line, replacements)

                    out_file_name.write(line[1:-1])

    if method == 3:
        # Method #3
        current_delimiter_count, delimiter_count = 0, 0
        prev_line, total_line, line = '', '', ''
        with open(in_file_name) as in_file:
            with open(out_file_name, 'w') as out_file_name:

                for line in in_file:
                    # print('prev_line: {} \nline: {}'.format(prev_line, line))
                    if not count:
                        delimiter_count = len(re.findall(reg_delimiter, line))
                        # print('delimiter_count', delimiter_count)
                    count = 1

                    current_delimiter_count = len(re.findall(reg_delimiter, line))
                    # print('current_delimiter_count', current_delimiter_count)

                    if current_delimiter_count == delimiter_count:
                        # print('all good!')
                        prev_line = ''
                        total_line = line

                        total_line = ''.join((';', total_line, ';'))

                        total_line = multireplace(total_line, replacements)

                        out_file_name.write(total_line[1:-1])
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
                            # print('am i here? total line is {}'.format(total_line))
                            prev_line = total_line
                            # print(prev_line)
                        else:
                            # print('is line correct?')
                            # print('total line is {} and current_delimiter_count is {}'.format(total_line, len(re.findall(reg_delimiter, total_line))))
                            # total_line += '\r\n'
                            # print('check here', total_line)

                            total_line = ''.join((';', total_line, ';'))

                            total_line = multireplace(total_line, replacements)

                            out_file_name.write(total_line[1:-1])
                            prev_line = ''
                            # print(total_line[1:-1])


if __name__ == '__main__':
    # source_folder = 'C:\\Users\\mrpc\\Documents\\test_files\\'
    # target_folder = 'C:\\Users\\mrpc\\Documents\\test_files\\files_treated\\'
    source_folder = sys.argv[1]
    target_folder = sys.argv[2]
    main(source_folder, target_folder)

