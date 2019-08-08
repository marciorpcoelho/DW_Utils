import fileinput
import sys
import re
import time
import pandas as pd
pd.set_option('display.expand_frame_repr', False)

regex_dict = {
    'new_lines': r'(\r\n|\r|\n)',
    'matches': r'"(\r\n|\r|\n)$',  # Encontra os CRLF no final de cada string, precedidos de aspas
    'plicas_middle': r'(?:[^;"])("{1,})(?:[^;"\r|;"\n])',  # Encontra as plicas que estão no meio do campo
    'plicas_left': r'[;]("{2,})',  # Encontra as plicas à esquerda do campo
    'plicas_right': r'("{2,})[;|\r|\n|\r\n]',  # Encontra as plicas à direita do campo
}


def main():
    file_name_1 = 'dbs/BIFD_SOHeaders_Comments_Orig'
    file_name_2 = 'dbs/BIFD_SOActivities_Orig'
    file_name_3 = 'D:\\BIFD_NLOpenAccountsMov'

    start = time.time()

    files = [file_name_1, file_name_2]

    for file in files:
        start_1 = time.time()
        # normalizer(file)
        print('Elapsed Time: {:.2f}'.format(time.time() - start_1))

    print('Elapsed Time: {:.2f}'.format(time.time() - start))


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


def normalizer(file_name):
    reg = re.compile(regex_dict['new_lines'])

    replacements = {
        regex_dict['plicas_middle']: 'middle',
        regex_dict['plicas_left']: 'left',
        regex_dict['plicas_right']: 'right',
    }

    for line in fileinput.input(file_name + '.csv', inplace=1):

        if file_name == 'dbs/BIFD_SOHeaders_Comments_Orig' and '"208";"72";"2018/OT3/922";"20180313";"3727";"21/03 (JR) HABLO CON CLIENTA' in line:
            line = ';"208";"72";"2018/OT3/922";"20180313";"3727";"21/03 (JR) HABLO CON CLIENTA, LE COMENTO QUE ES IMPOSIBLE TENERLO PARA HOY COMO ME DIJO ISRAEL, VAMOS A INTENTAR PARA EL VIERNES PERO ESTA COMLICADO, NO SE LO TOMA BIEN, HACE VARIOS DESPRECIOS, LE DIGO QUE ES UN LATERAL COMPLETO Y NO ES COSA DE DOS DIAS Y LA PIEZA SE HA RETRASADO EL PANEL DE PUERTA. DICE QUE HABLARA CON SU MADRE PARA QUE LLAME Y ""CANTE LAS 40"";'
        else:
            if not re.search(regex_dict['matches'], line):
                line = re.sub(reg, '', line)

            line = ''.join((';', line, ';'))

            line = multireplace(line, replacements)

        sys.stdout.write(line[1:-1])


if __name__ == '__main__':
    main()

