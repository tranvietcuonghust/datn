let prop_type = "Tất cả loại hình";
let district = "Quận/huyện";
let city = "Tỉnh/thành phố";
let ward = "Phường/xã";
let area_type = "Phân khúc diện tích";
let price_type = "Phân khúc giá cả";
let is_post = true;
let df = undefined;
let days_fetch = 20;
let LABEL = 'tin bài';

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

{
    let x = GetUrlParameter("post");
    if (x=="true") {
        is_post = true;
        LABEL = 'tin bài';
    } else {
        is_post = false;
        LABEL = 'lượt xem'
    }
}

function render_address_item_option(item) {
    return `<option value="${item[0]}">${item[1]}</option>`;
}

function render_list_districts(data) {
    if (data["status"]==true) {
        let district_size = data["data"].length;
        let append_content = "";
        for (let i=0; i<district_size; i++) {
            let item = data["data"][i];
            let child = render_address_item_option(item);
            append_content = append_content+child;
        }
        let old_content = $('#district-select').html()
        let content = old_content + append_content
        $('#district-select').html(content);
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

$('#type-select').on('change', function (event) {
    let value = $(this).find(":selected").text();
    $.ajax('/get_sub_type',
              {
                  method: 'post',
                  dataType: 'json',
                  data: {"type": value},
                  timeout: 300000,
                  success: function (data,status,xhr) {
                      let area_type = data["area_type"];
                      let price_type = data["price_type"];
                      $('#area-type').html(`<option selected value="-1">Phân khúc diện tích</option>`);
                      $('#price-type').html(`<option selected value="-1">Phân khúc giá cả</option>`);

                      let num_result = area_type.length;
                      let content = ""
                      for (let i=0; i<num_result; i++) {
                        content = content + `<option value="${i}">${area_type[i]}</option>`;
                      }
                      content = $('#area-type').html() + content;
                      $('#area-type').html(content);

                      num_result = price_type.length;
                      content = ""
                      for (let i=0; i<num_result; i++) {
                        content = content + `<option value="${i}">${price_type[i]}</option>`;
                      }
                      content = $('#price-type').html() + content;
                      $('#price-type').html(content);
                  },
                  error: function (jqXhr, textStatus, errorMessage) {
                      $('#area-type').html(`<option selected value="-1">Phân khúc diện tích</option>`);
                      $('#price-type').html(`<option selected value="-1">Phân khúc giá cả</option>`);
                  }
              });
});

$('#day-fetch').on('change', function (event) {
    days_fetch = $(this).val();
});

function update_filter() {
    prop_type = $('#type-select').find(":selected").text();
    district = $('#district-select').find(":selected").text();
    ward = $('#ward-select').find(":selected").text();
    area_type = $('#area-type').find(":selected").text();
    price_type = $('#price-type').find(":selected").text();
}

function compute_num_items_and_price_avg(group_col, keep_min_price=false) {
  let g_len = group_col.length;
  let sub_df = undefined;
  if (keep_min_price) {
        sub_df = df.groupby(group_col).agg({"sum_area": "sum", "sum_price": "sum", "count": "sum", "property_min_price": "mean"});
  } else {
        sub_df = df.groupby(group_col).agg({"sum_area": "sum", "sum_price": "sum", "count": "sum"});
  }
  let x = sub_df.apply((row) => parseInt(row[g_len+1]/row[g_len]), {axis:0});
  sub_df.addColumn({ "column": "Giá TB (/m2)", "values": x, inplace: true });
  sub_df.addColumn({ "column": `Số ${LABEL}`, "values": sub_df.column("count_sum"), inplace: true });
  return sub_df;
}

function plot_num_items_and_price_avg_region(add_col, add_name, num_idx_ele, price_idx_ele) {
  let sub_df = compute_num_items_and_price_avg([add_col]);
  sub_df.addColumn({ "column": " ", "values": sub_df.column(add_col), inplace: true });

  sub_df.sort_values({by: `Số ${LABEL}`, inplace: true, ascending: false})
  $(`#plot_${num_idx_ele}_title`).html(`Số lượng ${LABEL} trên từng `+add_name)
  sub_df.plot(`plot_${num_idx_ele}`).bar({x: ' ', y: `Số ${LABEL}`, displayModeBar: false});

  sub_df.sort_values({by: "Giá TB (/m2)", inplace: true, ascending: false})
  $(`#plot_${price_idx_ele}_title`).html("Giá trung bình với mỗi m2 trên từng "+add_name);
  sub_df.plot(`plot_${price_idx_ele}`).bar({x: ' ', y: 'Giá TB (/m2)', displayModeBar: false});
}

function plot_num_items_and_price_avg_type(num_idx_ele, price_idx_ele) {
  let sub_df = compute_num_items_and_price_avg(["property_type"]);
  sub_df.addColumn({ "column": " ", "values": sub_df.column("property_type"), inplace: true });

  sub_df.sort_values({by: `Số ${LABEL}`, inplace: true, ascending: false})
  $(`#plot_${num_idx_ele}_title`).html(`Số lượng ${LABEL} ở từng loại hình`)
  sub_df.plot(`plot_${num_idx_ele}`).bar({x: ' ', y: `Số ${LABEL}`, displayModeBar: false});

  sub_df.sort_values({by: "Giá TB (/m2)", inplace: true, ascending: false})
  $(`#plot_${price_idx_ele}_title`).html("Giá trung bình ở từng loại hình");
  sub_df.plot(`plot_${price_idx_ele}`).bar({x: ' ', y: 'Giá TB (/m2)', displayModeBar: false});
}

function plot_num_items_and_price_avg_time(num_idx_ele, price_idx_ele) {
  let sub_df = compute_num_items_and_price_avg(["property_date", "property_linux"]);
  sub_df.sort_values({by: "property_linux", inplace: true, ascending: true});
  sub_df.addColumn({ "column": " ", "values": sub_df.column("property_date"), inplace: true });

  $(`#plot_${num_idx_ele}_title`).html(`Số lượng ${LABEL} theo thời gian`)
  sub_df.plot(`plot_${num_idx_ele}`).line({x: ' ', y: `Số ${LABEL}`, displayModeBar: false});

  $(`#plot_${price_idx_ele}_title`).html("Giá trung bình theo thời gian");
  sub_df.plot(`plot_${price_idx_ele}`).line({x: ' ', y: 'Giá TB (/m2)', displayModeBar: false});
}

function plot_num_items_and_price_area_sub_type(num_idx_ele, price_idx_ele) {
  let sub_df = compute_num_items_and_price_avg(["property_area_type", "property_min_area"]);
  sub_df.addColumn({ "column": " ", "values": sub_df.column("property_area_type"), inplace: true });
  sub_df.sort_values({by: "property_min_area", inplace: true});

  $(`#plot_${num_idx_ele}_title`).html(`Số lượng ${LABEL} theo phân khúc diện tích`)
  sub_df.plot(`plot_${num_idx_ele}`).line({x: ' ', y: `Số ${LABEL}`, displayModeBar: false});

  $(`#plot_${price_idx_ele}_title`).html("Giá trung bình theo từng phân khúc diện tích");
  sub_df.plot(`plot_${price_idx_ele}`).line({x: ' ', y: 'Giá TB (/m2)', displayModeBar: false});
}

function plot_num_items_and_price_price_sub_type(num_idx_ele, price_idx_ele) {
  let sub_df = compute_num_items_and_price_avg(["property_price_type"], keep_min_price=true);
  sub_df.addColumn({ "column": " ", "values": sub_df.column("property_price_type"), inplace: true });
  sub_df.sort_values({by: "property_min_price_mean", inplace: true});

  $(`#plot_${num_idx_ele}_title`).html(`Số lượng ${LABEL} theo phân khúc giá cả`)
  sub_df.plot(`plot_${num_idx_ele}`).line({x: ' ', y: `Số ${LABEL}`, displayModeBar: false});

  $(`#plot_${price_idx_ele}_title`).html("Giá trung bình theo từng phân khúc giá cả");
  sub_df.plot(`plot_${price_idx_ele}`).line({x: ' ', y: 'Giá TB (/m2)', displayModeBar: false});
}

function render_statistical_data(data) {
      if (data["status"]==false) {
          $("#message").html("<h3>Không có dữ liệu ứng với khu vực bạn yêu cầu</h3>");
      } else {
          $("#message").html("<h3>Lấy dữ liệu thành công</h3>");
          data = data["data"];
          df = new dfd.DataFrame(data);
          let element_idx = 1;
          if (district == "Quận/huyện") {
            plot_num_items_and_price_avg_region("property_district", "quận/huyện", element_idx, element_idx+1);
            element_idx = element_idx + 2;
          } else {
            if (ward == "Phường/xã") {
                plot_num_items_and_price_avg_region("property_ward", "phường/xã", element_idx, element_idx+1);
                element_idx = element_idx + 2;
            }
          }
          if (prop_type == "Tất cả loại hình") {
            plot_num_items_and_price_avg_type(element_idx, element_idx+1);
            element_idx = element_idx + 2;
          } else {
            if (area_type == "Phân khúc diện tích") {
                plot_num_items_and_price_area_sub_type(element_idx, element_idx+1);
                element_idx = element_idx + 2;
            }
            if (price_type == "Phân khúc giá cả") {
                plot_num_items_and_price_price_sub_type(element_idx, element_idx+1);
                element_idx = element_idx + 2;
            }
          }
          plot_num_items_and_price_avg_time(element_idx, element_idx+1)
          element_idx = element_idx + 2;
      }
}

$('#apply-filter').on('click', function (event) {
  $("#message").html(`<h3>Đang lấy dữ liệu...</h3>`);
  update_filter();
  for (let i=1; i<9; i++) {
    $(`#plot_${i}_title`).html("");
    $(`#plot_${i}`).html("");
  }
  $.ajax('/get_statistic',
              {
                  method: 'post',
                  dataType: 'json',
                  data: {'post': is_post, "district": district, "ward": ward,
                         "type": prop_type, "day": days_fetch,
                         "area-type": area_type, "price-type": price_type},
                  timeout: 300000,
                  success: function (data,status,xhr) {
                      render_statistical_data(data);
                  },
                  error: function (jqXhr, textStatus, errorMessage) {
                      data = {"status": false, "message": "Kết nối server thất bại", data: []};
                      render_statistical_data(data);
                  }
              });
});

fetch_districts_data(render_list_districts);
$('#apply-filter').click();