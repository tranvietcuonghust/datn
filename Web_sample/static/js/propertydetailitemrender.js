function get_near_by_item_html(item) {
    let item_id = item["_id"];
    let item_title = title_format(item["property_title"]);
    let item_detail = detail_format(item["property_detail"]);
    let item_price = price_format(item["property_price"]);
    let item_area = area_format(item["property_area"]);
    let item_address = address_format(item["property_address"]);
    let item_date = item["property_date"];
    let item_type = item["property_type"];
    let item_image = "/static/img/item/1.jpg";
    if (item["property_images"].length>0) {
        item_image = item["property_images"][0];
    }
    let element = `<div class="col-md-6" id="${item_id}">
                  <article class="aa-properties-item">
                    <a href="/item_detail?id=${item_id}" class="aa-properties-item-img">
                      <img src="${item_image}" onerror="this.src='/static/img/slider/1.jpg'" alt="img">
                    </a>
                    <div class="aa-tag sold-out">
                      NearBy
                    </div>
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
                        <h3><a href="/item_detail?id=${item_id}">${item_title}</a></h3>
                        <p>${item_detail}</p>
                      </div>
                      <div class="aa-properties-detial">
                        <span class="aa-price">${item_price}</span>
                        <a href="/item_detail?id=${item_id}" class="aa-secondary-btn">View Details</a>
                      </div>
                    </div>
                  </article>
            </div>`;
    return element;
}

function render_near_by_items_data(data) {
    if (data["status"]==true) {
        let len = data["data"].length;
        let content = "";
        for (let i=0; i<len; i++) {
            let item = data["data"][i];
            let child = get_near_by_item_html(item);
            content = content + child;
        }
        $('#aa-properties .container .aa-properties-content .aa-properties-details .aa-nearby-properties .aa-nearby-properties-area .row').html(content);
    } else {
        $('#aa-properties .container .aa-properties-content .aa-properties-details .aa-nearby-properties .aa-nearby-properties-area .row').html(data["message"]);
    }
}

function render_near_by() {
    let ward = $('#aa-properties .container .aa-properties-content .aa-properties-details .aa-nearby-properties .info div').eq(0).html();
    let district = $('#aa-properties .container .aa-properties-content .aa-properties-details .aa-nearby-properties .info div').eq(1).html();
    let province = $('#aa-properties .container .aa-properties-content .aa-properties-details .aa-nearby-properties .info div').eq(2).html();
    let type = $('#aa-properties .container .aa-properties-content .aa-properties-details .aa-nearby-properties .info div').eq(3).html();
    fetch_near_by_items_data(ward=ward, district=district, province=province, type=type, callback_func=render_near_by_items_data);
}

render_near_by();
fetch_districts_data(render_list_districts);

{
    if( $("#user_id").length ) {
        let user_id = $("#user_id").html();
        let item_id = $("#property_id").html();
        $.ajax('/write_log',
                {
                    method: 'post',
                    dataType: 'json',
                    data: {'userId': user_id, "itemId": item_id},
                    timeout: 60000,
                    success: function (data,status,xhr) {

                    },
                    error: function (jqXhr, textStatus, errorMessage) {

                    }
                }
        );
    }
}