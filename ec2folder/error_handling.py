#******************** Error type and Line number  **************************************************
#*********************************************************************************************************

import sys

def error_handling():
    if sys.exc_info()[2] is not None:
        s= '{} ,{}, line: {}'.format(sys.exc_info()[0],
                                      sys.exc_info()[1],
                                      sys.exc_info()[2].tb_lineno)
    else:
        s=''
    return s


