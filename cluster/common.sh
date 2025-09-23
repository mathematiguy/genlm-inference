# common.sh

# Function to display usage
usage() {
    echo "Usage: $0 [--stage|-s <stage>]"
    exit 1
}

# Function to parse arguments
parse_arguments() {
    STAGE=""
    while [[ $# -gt 0 ]]; do
        key="$1"
        case $key in
            --stage|-s)
            STAGE="$2"
            shift # past argument
            shift # past value
            ;;
            *)
            usage
            ;;
        esac
    done
}
