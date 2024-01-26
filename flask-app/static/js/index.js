$(document).ready(function () {
    // Disable the "Run" button initially
    $('#runButton').prop('disabled', true);

    // Add an event listener to check for input changes
    $('form input').on('change', function () {
        updateRunButtonState();
    });

    // Function to update the "Run" button state
    function updateRunButtonState() {
        // Check if at least one input has a value
        var atLeastOneInputHasValue = $('form input').toArray().some(function (input) {
            return $(input).val().trim() !== '';
        });

        // Enable or disable the "Run" button based on the condition
        $('#runButton').prop('disabled', !atLeastOneInputHasValue);
    }
});
