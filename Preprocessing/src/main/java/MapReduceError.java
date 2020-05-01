public class MapReduceError {
    enum Errors {
        SEQ_FILE_IS_NOT_GZIP,
        SITE_FILE_IS_NOT_GZIP,
        VCF_IS_NOT_GZIP,
        FILE_FORMAT_NOT_SUPPORTED,
        GROUPS_FILE_TOO_MANY_COLUMNS,
        GROUPS_FILE_MISSING_COLUMNS,
        SEQ_FILE_TOO_FEW_COLUMNS,
        SEQ_FILE_MISSING_COLUMNS,
        SEQ_FILE_INCONSISTENT_COLUMNS,
        SEQ_SAMPLE_NOT_IN_GROUP,
        SITE_FILE_TOO_FEW_COLUMNS,
        SITE_FILE_HEADER_NO_CHR,
        SITE_FILE_HEADER_NO_POS,
        SITE_FILE_HEADER_NO_ID,
        SITE_FILE_HEADER_NO_REF,
        SITE_FILE_HEADER_NO_ALT,
        SITE_FILE_LOCI_NOT_IN_SEQ,
        VCF_SAMPLE_NOT_IN_GROUP,
        VCF_NO_HEADER,
        VCF_HEADER_NO_SAMPLES,
        VCF_HEADER_NO_CHROM,
        VCF_HEADER_NO_POS,
        VCF_HEADER_NO_ID,
        VCF_HEADER_NO_REF,
        VCF_HEADER_NO_ALT,
        VCF_HEADER_NO_QUAL,
        VCF_HEADER_NO_FILTER,
        VCF_HEADER_NO_INFO,
        VCF_HEADER_NO_FORMAT,
        VCF_HIGH_PLOIDITY
    }

    static String getMessage(Errors error) {
        switch (error) {
            case SEQ_FILE_IS_NOT_GZIP: return "Study sequence file must be compressed using Gzip!";
            case SITE_FILE_IS_NOT_GZIP: return "Study site file must be compressed using Gzip!";
            case VCF_IS_NOT_GZIP: return "Input VCF(s) must be compressed using Gzip!";
            case FILE_FORMAT_NOT_SUPPORTED: return "Provided study genotypes file format is not supported!";
            case GROUPS_FILE_TOO_MANY_COLUMNS: return "Study groups file has line(s) with more than two columns!";
            case GROUPS_FILE_MISSING_COLUMNS: return "Study groups file has line(s) with less than two columns!";
            case SEQ_FILE_TOO_FEW_COLUMNS: return "One or more lines in study sequence file have less than five columns!";
            case SEQ_FILE_MISSING_COLUMNS: return "Columns are missing in one or more lines in study sequence file!";
            case SEQ_FILE_INCONSISTENT_COLUMNS: return "One or more lines in study sequence file have different number of columns!";
            case SEQ_SAMPLE_NOT_IN_GROUP: return "One or more individuals from study sequence file are not present in study groups file!";
            case SITE_FILE_TOO_FEW_COLUMNS: return "Site file has less than five columns!";
            case SITE_FILE_HEADER_NO_CHR: return "No 'CHR' column in site file!";
            case SITE_FILE_HEADER_NO_POS: return "No 'POS' column in site file!";
            case SITE_FILE_HEADER_NO_ID: return "No 'ID' column in site file!";
            case SITE_FILE_HEADER_NO_REF: return "No 'REF' column in site file!";
            case SITE_FILE_HEADER_NO_ALT: return "No 'ALT' column in site file!";
            case SITE_FILE_LOCI_NOT_IN_SEQ: return "Study sequence and site files have different number of loci!";
            case VCF_SAMPLE_NOT_IN_GROUP: return "One or more individuals from study VCF file(s) are not present in study groups file!";
            case VCF_NO_HEADER: return "VCF file has no header!";
            case VCF_HEADER_NO_SAMPLES: return "VCF file has no individuals!";
            case VCF_HEADER_NO_CHROM: return "No '#CHROM' column in VCF file!";
            case VCF_HEADER_NO_POS: return "No 'POS' column in VCF file!";
            case VCF_HEADER_NO_ID: return "No 'ID' column in VCF file!";
            case VCF_HEADER_NO_REF: return "No 'REF' column in VCF file!";
            case VCF_HEADER_NO_ALT: return "No 'ALT' column in VCF file!";
            case VCF_HEADER_NO_QUAL: return "No 'QUAL' column in VCF file!";
            case VCF_HEADER_NO_FILTER: return "No 'FILTER' column in VCF file!";
            case VCF_HEADER_NO_INFO: return "No 'INFO' column in VCF file!";
            case VCF_HEADER_NO_FORMAT: return "No 'FORMAT' column in VCF file!";
            case VCF_HIGH_PLOIDITY: return "Only up to 126-ploid organisms are supported!";
            default: return "Error!";
        }
    }
}
