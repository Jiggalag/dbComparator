def convert_to_list(structure_to_convert):
    result_list = []
    for item in structure_to_convert:
        if type(item) is dict:
            key = list(item.keys())[0]
            result_list.append(item.get(key))
        else:
            result_list.append(item)
    try:
        result_list.sort()
    except TypeError:
        print('Raised TypeError during list sorting')
    return result_list
