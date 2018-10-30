# you can write to stdout for debugging purposes, e.g.
# print("this is a debug message")
def solution(S):
    lines = S.split('\n')
    places = {'o': [], 'c': {}}
    cities = {}
    for line in lines:
        arr_line = line.split(',')
        format = arr_line[0].split('.')[1]
        city = arr_line[1].replace(' ', '')
        s_date_time = arr_line[2].split(' ')
        arr_s_date = s_date_time[1].split('-')
        arr_s_time = s_date_time[2].split(':')
        place = int(arr_s_date[0] + arr_s_date[1] + arr_s_date[2] + arr_s_time[0] + arr_s_time[1] + arr_s_time[2])
        if not cities.get(city):
            cities[city] = {'c': 0, 'm': {}, 'p': 1}
        cities[city]['c'] += 1
        cities[city]['m'][place] = {'f': format, 'c': cities[city]['c']}
        if len(str(cities[city]['c'])) > cities[city]['p']:
            cities[city]['p'] = len(str(cities[city]['c']))

        if not places['c'].get(place):
            places['c'][place] = city
        places['o'].append(place)

    places['o'].sort()

    result = ''

    for d in places['o']:
        city = places['c'][d]
        city_obj = cities[city]
        format = city_obj['m'][d]['f']
        num = city_obj['m'][d]['c']
        str_num = str(num)
        i = city_obj['p'] - len(str_num)
        s = ''
        for j in range(i):
            s += '0'
        file_name = city + s + str_num + '.' + format
        result += file_name + '\n'

    print(result)

    return result

S = 'asdsad.jpg'
solution(S)