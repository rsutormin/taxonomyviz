package taxonomyviz;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;

@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({
    "taxid",
    "title",
    "children"
})
public class TaxNode {
    public int taxid;
    public String title;
    public int hidden = 0;
    @JsonIgnore
    public int layer = 0;
    @JsonIgnore
    public int lpos = 0;
    @JsonIgnore
    public int ind = 0;
    @JsonIgnore
    public int maxind = 0;
    public List<TaxNode> children;

    public TaxNode() {}
    
    public TaxNode(int taxId, String name) {
        this.taxid = taxId;
    	this.title = name;
    	this.children = null;
    }
}
