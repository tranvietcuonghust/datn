function fetch_recommend_items_data(user_id, callback_func) {
    return $.ajax('/get_recommend',
                {
                    method: 'post',
                    dataType: 'json',
                    data: {'userId': user_id},
                    timeout: 60000,
                    success: function (data,status,xhr) {
                        callback_func(data);
                    },
                    error: function (jqXhr, textStatus, errorMessage) {
                        data = {"status": false, "message": "Kết nối server thất bại", data: []};
                        callback_func(data);
                    }
                }
            );
}

function fetch_items_data(limit, offset, sort, filter, callback_func) {
    let data = {
                "limit": limit,
                "offset": offset,
                "sort": sort,
                "filter": filter,
            }
            console.log("Sending AJAX request with data:", data);
            return $.ajax('/find_items', {
                method: 'post',
                dataType: 'json',
                data: {'query': JSON.stringify(data)},
                timeout: 60000,
                success: function (data, status, xhr) {
                    console.log("fetch_items_data: ", data);
                    callback_func(data);
                },
                error: function (jqXhr, textStatus, errorMessage) {
                    console.log("fetch_items_data: ", textStatus, errorMessage);
                    data = {"status": false, "message": "Kết nối server thất bại", data: data};
                    callback_func(data);
                }
            });
}

function count_items_data(filter, callback_func) {
    let data = {
                "filter": filter,
            }
    return $.ajax('/count_items',
                {
                    method: 'post',
                    dataType: 'json',
                    data: {'query': JSON.stringify(data)},
                    timeout: 60000,
                    success: function (data,status,xhr) {
                        callback_func(data);
                    },
                    error: function (jqXhr, textStatus, errorMessage) {
                        data = {"status": false, "message": "Kết nối server thất bại", data: 0};
                        callback_func(data);
                    }
                }
            );
}

function fetch_newest_items_data(callback_func, limit=15, offset=0) {
    let data = {
                "limit": limit,
                "offset": offset,
                "sort": {"property_linux": -1},
            }
    return $.ajax('/find_items',
                {
                    method: 'post',
                    dataType: 'json',
                    data: {'query': JSON.stringify(data)},
                    timeout: 60000,
                    success: function (data,status,xhr) {
                        callback_func(data);
                    },
                    error: function (jqXhr, textStatus, errorMessage) {
                        data = {"status": false, "message": "Kết nối server thất bại", data: []};
                        callback_func(data);
                    }
                }
            );
}

function fetch_near_by_items_data(ward, district, province, type, callback_func) {
    let send_data = {
                "ward": ward,
                "district": district,
                "province": province,
                "type": type,
            }
    return $.ajax('/near_by',
                {
                    method: 'post',
                    dataType: 'json',
                    data: send_data,
                    timeout: 60000,
                    success: function (data,status,xhr) {
                        callback_func(data);
                    },
                    error: function (jqXhr, textStatus, errorMessage) {
                        data = {"status": false, "message": "Kết nối server thất bại", data: []};
                        callback_func(data);
                    }
                }
            );
}

function fetch_districts_data(city, callback_func) {
    let send_data = {
                "province": city,
            }
    return $.ajax('/get_districts',
                {
                    method: 'post',
                    dataType: 'json',
                    data: send_data,
                    timeout: 60000,
                    success: function (data,status,xhr) {
                        callback_func(data);
                    },
                    error: function (jqXhr, textStatus, errorMessage) {
                        data = {"status": false, "message": "Kết nối server thất bại", data: []};
                        callback_func(data);
                    }
                }
            );
}


function fetch_cities_data(callback_func) {
    let send_data = {
                "province": 1,
            }
    return $.ajax('/get_cities',
                {
                    method: 'post',
                    dataType: 'json',
                    data: send_data,
                    timeout: 60000,
                    success: function (data,status,xhr) {
                        callback_func(data);
                    },
                    error: function (jqXhr, textStatus, errorMessage) {
                        data = {"status": false, "message": "Kết nối server thất bại", data: []};
                        callback_func(data);
                    }
                }
            );
}

function fetch_wards_data(district, callback_func) {
    let send_data = {
                "district": district,
            }
    return $.ajax('/get_wards',
                {
                    method: 'post',
                    dataType: 'json',
                    data: send_data,
                    timeout: 60000,
                    success: function (data,status,xhr) {
                        callback_func(data);
                    },
                    error: function (jqXhr, textStatus, errorMessage) {
                        data = {"status": false, "message": "Kết nối server thất bại", data: []};
                        callback_func(data);
                    }
                }
            );
}