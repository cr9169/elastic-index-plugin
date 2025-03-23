package org.example;

import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestToXContentListener;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

public class CustomIndexRestHandler extends BaseRestHandler {

    @Override
    public String getName() {
        return "custom_index_rest_handler";
    }

    @Override
    public List<Route> routes() {
        return Collections.singletonList(
                new Route(RestRequest.Method.POST, "/_custom_index")
        );
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) {
        // כאן נגדיר את שם האינדקס
        IndexRequest indexRequest = new IndexRequest("target_index");
        // שים לב: אנו מעבירים את תוכן ה-JSON שהגיע בבקשה ישירות ל-IndexRequest
        indexRequest.source(request.content(), request.getXContentType());

        // החזרה של Lambda שמבצעת את האינדוקס באופן א-סינכרוני, וכותבת חזרה את התגובה
        return channel ->
                client.index(indexRequest, new RestToXContentListener<>(channel));
    }
}