function price_format(price) {
    let price_str = price.toString().replace(/\B(?=(\d{3})+(?!\d))/g, ",");
    price_str = price_str + " VNĐ";
    return price_str
}

function area_format(area) {
    if (area === undefined || area === null) {
        return "N/A";
    }
    return area.toString() + " m²";
}

function address_format(address) {
    if (address === undefined || address === null) {
        return "N/A";
    }
    let len = address?.length;
    if (len<=80) {
        let padding = 80 - len;
        let pad_str = ""
        for (let i=1; i<=padding; i++) {
            pad_str = pad_str + "&nbsp";
        }
        return address + pad_str;
    } else {
        return address.substring(0,77)+"...";
    }
}

function title_format(title) {
    let len = title.length;
    if (len<=70) {
        let padding = 70 - len;
        let pad_str = ""
        for (let i=1; i<=padding; i++) {
            pad_str = pad_str + "&nbsp";
        }
        return title + pad_str;
    } else {
        return title.substring(0,67)+"...";
    }
}

function detail_format(detail) {
    let len = detail.length;
    if (len<=170) {
        let padding = 170 - len;
        let pad_str = ""
        for (let i=1; i<=padding; i++) {
            pad_str = pad_str + "&nbsp";
        }
        return detail + pad_str;
    } else {
        return detail.substring(0,167)+"...";
    }
}

function number_str_format(number_str) {
    let number = parseInt(number_str);
    if (isNaN(number)) {
        return "";
    } else {
        return number.toString();
    }
}