import moment from "moment";

export class DateFormatValueConverter {
    toView(value) {
        if (value > 9999999999)
            value = value / 1000;
        return moment.unix(value).format('M/D/YYYY h:mm:ss a').padStart(22);
    }
}