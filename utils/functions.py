# Geometry stuff for plotting 

def color_msg(msg, color = "none", indentLevel = 0):
    """ Prints a message with ANSI coding so it can be printout with colors """
    codes = {
        "none" : "0m",
        "green" : "1;32m",
        "red" : "1;31m",
        "blue" : "1;34m",
        "yellow" : "1;35m"
    }

    indentStr = ""
    if indentLevel == 0: indentStr = ">>"
    if indentLevel == 1: indentStr = "+"
    if indentLevel == 2: indentStr = "*"
    if indentLevel == 3: indentStr = "-->"

    
    print("\033[%s%s %s \033[0m"%(codes[color], "  "*indentLevel + indentStr, msg))
    return

