function render_address_item_option(item) {
    return `<option value="${item[0]}">${item[1]}</option>`
}

function render_list_cities(data) {
    if (data["status"]==true) {
        let city_size = data["data"].length;
        let append_content = "";
        for (let i=0; i<city_size; i++) {
            let item = data["data"][i];
            let child = render_address_item_option(item);
            append_content = append_content+child;
        }
        let old_content = $('#city').html()
        let content = old_content + append_content
        $('#city').html(content);
    }
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
        let old_content = $('#district').html()
        let content = old_content + append_content
        $('#district').html(content);
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
        let old_content = $('#ward').html()
        let content = old_content + append_content
        $('#ward').html(content);
    }
}

$('#city').on('change', function (event) {
    let value = $(this).val();
    $('#district').children().remove();
    $('#district').append(`<option value="-1" selected>Quận/huyện</option>`);
    if (value == -1) {
        $('#district').prop('disabled',true);
    } else {
        $('#district').prop('disabled',false);
        fetch_districts_data(city=value, callback_func=render_list_districts)
    }
});

$('#district').on('change', function (event) {
    let value = $(this).val();
    $('#ward').children().remove();
    $('#ward').append(`<option value="-1" selected>Phường/xã</option>`);
    if (value == -1) {
        $('#ward').prop('disabled',true);
    } else {
        $('#ward').prop('disabled',false);
        fetch_wards_data(district=value, callback_func=render_list_wards)
    }
});

$("#area").on('keyup', function(event) {
    let value = $(this).val();
    value = number_str_format(value);
    $(this).val(value);
});

$("#price").on('keyup', function(event) {
    let value = $(this).val();
    value = number_str_format(value);
    $(this).val(value);
});

let num_files = 0;
function readURL(input) {
  num_files = input.files.length;
  if (num_files>0) {
      $("#add").css("display","none");
      for (let i=0; i<num_files; i++) {
          var reader = new FileReader();
          let current = $('#preview').html();
          let append = `<img id="image_${i}" src="">`
          $('#preview').html(current+append);
          reader.onload = function (e) {
              $(`#image_${i}`).attr('src', e.target.result);
          }
          reader.readAsDataURL(input.files[i]);
      }
      $("#delete").css("display", "block");
  }
}
$("#images").change(function(){
  readURL(this);
});
$("#add").on("click", function(){
  $("#images").click();
});
$("#delete").on("click", function() {
  $(this).css("display", "none");
  $("#images").val(null);
  for (let i=0; i<num_files; i++) {
    $(`#image_${i}`).remove();
  }
  $("#add").css("display", "block");
  $("#add").on("click", function(){
      $("#images").click();
  });
});
fetch_cities_data(callback_func=render_list_cities);
fetch_districts_data(callback_func=render_list_districts);