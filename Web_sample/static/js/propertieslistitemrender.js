let num_result = 0;
let current_page = 1;
let max_page = 1;
let num_per_page = 20;

const currentUrlString = window.location.href;

// Create a URL object from the current URL
const currentUrl = new URL(currentUrlString);

// Create a URLSearchParams object from the URL
const params = new URLSearchParams(currentUrl.search);

// Initialize the filter object
let filter = {};

// Extract and add the parameters to the filter object
filter.city = params.get('city');
filter.district = params.get('district');
filter.ward = params.get('ward');

// Optional: log the filter object to verify
console.log(currentUrl);

let sort = {"property_linux": -1};

function display_num_result(data) {
    num_result = data["data"];
    $("#num-results").html(`Có ${num_result} kết quả trả về`);
    max_page = Math.ceil(num_result/num_per_page);
    current_page = 1;
    $("#current-page").html(current_page);
}

function get_number_value(selector) {
    let value = $(selector).val();
    if (value=="") {
        return 0;
    } else {
        let value_num = parseInt(value);
        if (isNaN(value_num)) {
            return 0;
        } else {
            return value_num;
        }
    }
}

function get_filter_item_html(item) {
    let item_id = item["URL"];
    let item_title = title_format(item["Title"]);
    let item_detail = detail_format(item["Description"]);
    let item_price = price_format(item["Price"]);
    let item_area = area_format(item["Area"]);
    let item_address = address_format(item["Address"]);
    let item_date = item["Created_date"];
    let item_type = item["RE_Type"];
    let item_image = "/static/img/item/1.jpg";
    if(!item["property_images"] || item["property_images"].length == 0) {
        item_image = "/static/img/item/1.jpg";
    }
    if (item["property_images"]?.length>0) {
        item_image = item["property_images"][0];
    }
    let content =  `<li id="${item_id}">
                      <article class="aa-properties-item">
                        <a class="aa-properties-item-img" href="/item_detail?id=${item_id}" target="_blank">
                          <img alt="img" src="${item_image}" onerror="this.src='/static/img/slider/1.jpg'">
                        </a>
                        <div class="aa-properties-item-content">
                          <div class="aa-properties-info">
                                <span>${item_type}</span>
                                <span>${item_area}</span>
                                <span>${item_date}</span>
                          </div>
                          <div class="aa-properties-info">
                            <i class="fa fa-map-marker"></i> ${item_address}
                          </div>
                          <div class="aa-properties-about">
                                <h3><a href="/item_detail?id=${item_id}" target="_blank">${item_title}</a></h3>
                                <p>${item_detail}</p>
                          </div>
                          <div class="aa-properties-detial">
                                <span class="aa-price">${item_price}</span>
                                <a href="/item_detail?id=${item_id}" class="aa-secondary-btn" target="_blank">View Details</a>
                          </div>
                        </div>
                      </article>
                    </li>`;
    return content;
}

function render_filter_list_items(data) {
    if (data["status"]==true) {
        let len = data["data"].length;
        let content = "";
        for (let i=0; i<len; i++) {
            let item = data["data"][i];
            let child = get_filter_item_html(item);
            content = content + child;
        }
        $('#aa-properties .container .aa-properties-content .aa-properties-content-body .aa-properties-nav').html(content);
    } else {
        $('#aa-properties .container .aa-properties-content .aa-properties-content-body .aa-properties-nav').html(data["message"]);
    }
}

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

function render_filter_list_items_next(data) {
    render_filter_list_items(data);
    current_page = current_page+1;
    $("#current-page").html(current_page);
}

function render_filter_list_items_prev(data) {
    render_filter_list_items(data);
    current_page = current_page-1;
    $("#current-page").html(current_page);
}

$('#next-page').on('click', function(event) {
    if (current_page<max_page) {
        let offset = current_page*num_per_page;
        fetch_items_data(limit=num_per_page, offset=offset, sort=sort, filter=filter, callback_func=render_filter_list_items_next);
    }
});

$('#prev-page').on('click', function(event) {
    if (current_page>1) {
        let offset = (current_page-2)*num_per_page;
        fetch_items_data(limit=num_per_page, offset=offset, sort=sort, filter=filter, callback_func=render_filter_list_items_prev);
    }
});

update_filter();
fetch_cities_data(render_list_cities);
// fetch_districts_data(render_list_districts);
setTimeout(function() {$("#apply-filter").click();}, 2000);