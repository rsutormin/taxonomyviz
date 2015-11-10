package taxonomyviz;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;

@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({
    "name",
    "user",
    "fields",
    "data"
})
public class Track {
	public String name;
	public String user;
	public List<String> fields;
	public List<Object> data;
}
