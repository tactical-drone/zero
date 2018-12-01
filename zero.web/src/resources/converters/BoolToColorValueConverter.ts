class BoolToColorValueConverter {
    toView(value) {
        if (value)
            return "green";
        else
            return "red";
    }
}