
filter = {}
function update_filter() {
    let min_area = GetUrlParameter("minarea");
    if (min_area!="") {
        $('#min-area').val(min_area);
    }
    let max_area = GetUrlParameter("maxarea");
    if (max_area!="") {
        $('#max-area').val(max_area);
    }
    let min_price = GetUrlParameter("minprice");
    if (min_price!="") {
        $('#min-price').val(min_price);
    }
    let max_price = GetUrlParameter("maxprice");
    if (max_price!="") {
        $('#max-price').val(max_price);
    }
    let sort = GetUrlParameter("sort");
    if (sort!="") {
        $('#sort-mode').val(sort);
    }
    let type = GetUrlParameter("type");
    if (type!="") {
        $("#type-select").val(type);
    }
}

function render_address_item_option(item, selected_item=-1) {
    if (item[0]!=selected_item) {
        return `<option value="${item[0]}">${item[1]}</option>`
    } else {
        return `<option value="${item[0]}" selected>${item[1]}</option>`
    }
}


function render_list_cities(data) {
    if (data["status"]==true) {
        let city = GetUrlParameter("city");
        let city_code = -1;
        if (city != "") {
            city_code = city;
        }
        let city_size = data["data"].length;
        let append_content = "";
        for (let i=0; i<city_size; i++) {
            let item = data["data"][i];
            let child = render_address_item_option(item, city_code);
            append_content = append_content+child;
        }
        let old_content = $('#city-select').html()
        let content = old_content + append_content
        $('#city-select').html(content);
        if (city_code != -1) {
            fetch_districts_data(city=city, callback_func=render_list_districts_and_update)
        }
    }
}

function render_list_districts(data) {
    if (data["status"]==true) {
        let district = GetUrlParameter("district");
        let district_code = -1;
        if (district != "") {
            district_code = parseInt(district);
        }
        let district_size = data["data"].length;
        let append_content = "";
        for (let i=0; i<district_size; i++) {
            let item = data["data"][i];
            let child = render_address_item_option(item, district_code);
            append_content = append_content+child;
        }
        let old_content = $('#district-select').html()
        let content = old_content + append_content
        $('#district-select').html(content);
        if (district_code != -1) {
            fetch_wards_data(district=district_code, callback_func=render_list_wards_and_update)
        }
    }
}

function render_list_wards(data) {
    if (data["status"]==true) {
        let district_size = data["data"].length;
        let append_content = "";
        for (let i=0; i<district_size; i++) {
            let item = data["data"][i];
            let child = render_address_item_option(item);
            append_content = append_content+child;
        }
        let old_content = $('#ward-select').html()
        let content = old_content + append_content
        $('#ward-select').html(content);
    }
}

function render_list_wards_and_update(data) {
    if (data["status"]==true) {
        let district = GetUrlParameter("ward");
        let district_code = -1;
        if (district != "") {
            district_code = parseInt(district);
        }
        let district_size = data["data"].length;
        let append_content = "";
        for (let i=0; i<district_size; i++) {
            let item = data["data"][i];
            let child = render_address_item_option(item, district_code);
            append_content = append_content+child;
        }
        let old_content = $('#ward-select').html()
        let content = old_content + append_content
        $('#ward-select').html(content);
    }
}

function render_list_districts_and_update(data) {
    if (data["status"]==true) {
        let city = GetUrlParameter("district");
        let city_code = -1;
        if (city != "") {
            city_code = city;
        }
        let city_size = data["data"].length;
        let append_content = "";
        for (let i=0; i<city_size; i++) {
            let item = data["data"][i];
            let child = render_address_item_option(item, city_code);
            append_content = append_content+child;
        }
        let old_content = $('#district-select').html()
        let content = old_content + append_content
        $('#district-select').html(content);
    }
}

function GetUrlParameter(sParam) {
    let sPageURL = window.location.search.substring(1);
    let sURLVariables = sPageURL.split('&');
    for (let i = 0; i < sURLVariables.length; i++) {
        let sParameterName = sURLVariables[i].split('=');
        if (sParameterName[0] == sParam) {
            return sParameterName[1];
        }
    }
    return "";
}

$('#city-select').on('change', function (event) {
    let value = $(this).val();
    $('#district-select').children().remove();
    $('#district-select').append(`<option value="-1" selected>Quan/huyen</option>`);
    if (value == -1) {
        $('#district-select').prop('disabled',true);
    } else {
        $('#district-select').prop('disabled',false);
        fetch_districts_data(city=value, callback_func=render_list_districts)
    }
});

$('#district-select').on('change', function (event) {
    let value = $(this).val();
    $('#ward-select').children().remove();
    $('#ward-select').append(`<option value="-1" selected>Phường/xã</option>`);
    if (value == -1) {
        $('#ward-select').prop('disabled',true);
    } else {
        $('#ward-select').prop('disabled',false);
        fetch_wards_data(district=value, callback_func=render_list_wards)
    }
});

$("#min-area").on('keyup', function(event) {
    let value = $(this).val();
    value = number_str_format(value);
    $(this).val(value);
});

$("#max-area").on('keyup', function(event) {
    let value = $(this).val();
    value = number_str_format(value);
    $(this).val(value);
});

$("#min-price").on('keyup', function(event) {
    let value = $(this).val();
    value = number_str_format(value);
    $(this).val(value);
});
$("#max-price").on('keyup', function(event) {
    let value = $(this).val();
    value = number_str_format(value);
    $(this).val(value);
});

$('#apply-filter').on('click', function (event) {
    filter = {};
    let search_value = $('#search-text').val();
    if (search_value != "") {
        filter["property_search"] = search_value;
    }

    let type_value = $('#type-select').val();
    if (type_value != "-1") {
        let type_label = $('#type-select').find(":selected").text();
        filter["RE_Type"] = type_label;
    }

    let city_value = $('#city-select').val();
    if (city_value != "-1") {
        let city_label = $('#city-select').find(":selected").text();
        filter["City"] = city_label;
    }
    let district_value = $('#district-select').val();
    if (district_value != "-1") {
        let district_label = $('#district-select').find(":selected").text();
        filter["District"] = district_label;
    }
    let ward_value = $('#ward-select').val();
    if (ward_value != "-1") {
        let ward_label = $('#ward-select').find(":selected").text();
        filter["Ward"] = ward_label;
    }

    let min_price = get_number_value("#min-price");
    let max_price = get_number_value("#max-price");
    if (min_price!=0) {
        if (max_price!=0) {
            if (min_price<max_price) {
                filter["Price"] = {"$gt": min_price, "$lt": max_price}
            }
        } else {
            filter["Price"] = {"$gt": min_price}
        }
    } else {
        if (max_price!=0) {
            filter["Price"] = {"$lt": max_price}
        }
    }
    let min_area = get_number_value("#min-area");
    let max_area = get_number_value("#max-area");
    if (min_area!=0) {
        if (max_area!=0) {
            if (min_area<max_area) {
                filter["Acreage"] = {"$gt": min_area, "$lt": max_area}
            }
        } else {
            filter["Acreage"] = {"$gt": min_area}
        }
    } else {
        if (max_area!=0) {
            filter["Acreage"] = {"$lt": max_area}
        }
    }
    sort = {};
    let sort_value = $('#sort-mode').val();
    if (sort_value == "0") {
        sort["Price"] = -1;
    } else if (sort_value == "1") {
        sort["Price"] = 1;
    } else if (sort_value == "2") {
        sort["Acreage"] = 1;
    } else if (sort_value == "3") {
        sort["Acreage"] = -1;
    } else {
        sort["property_linux"] = -1;
    }
    count_items_data(filter=filter, callback_func=display_num_result);
    fetch_items_data(limit=num_per_page, offset=0, sort=sort, filter=filter, callback_func=render_filter_list_items)
});

// update_filter();
// setTimeout(function() {$("#apply-filter").click();}, 2000);