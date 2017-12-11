package simpledb;

/**
 * A class that represents the private part of the OPE key pair.
 * 
 */
class OPE_PrivateKey {
    private final OPE ope;

    OPE_PrivateKey(OPE ope) {
        this.ope = ope;
    }

    public OPE getOPE() {
        return this.ope;
    }
}
