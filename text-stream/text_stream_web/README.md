# text-stream - Web UI

A purely static web application for managing and operating the text stream system.

## Features

- WebSocket connection management
- Stream management (create, delete, query, search)
- Content editing (read, write, append, insert, delete, replace)
- Semantic-layer search (search, box-selection search)
- Real-time subscriptions (subscribe, unsubscribe, change notifications)
- Metadata management (description, tags, rename)
- Version control (undo operations)

## Quick Start (Recommended)

### One-command startup

```bash
python start.py
```

This starts both:
- text-stream (WebSocket server, port 9876)
- the HTTP server for the web UI (port 8080)

Once started, it automatically opens the browser at http://localhost:8080

### Manual startup

1. Start the text stream system:
   ```python
   from text_stream_system.system import TextStreamSystem
   import asyncio

   system = asyncio.run(TextStreamSystem.start(port=9876))
   ```

2. Open `text_stream_web/index.html` in the browser

3. The system connects automatically to `ws://localhost:9876`

## Configuration

Configure via URL parameters:

```
index.html?port=9876&token=your_token
```

Options:
- `port`: WebSocket server port (default: 9876)
- `token`: Authentication token (optional)

The configuration is saved to local storage automatically.

## File Structure

```
text_stream_web/
├── index.html              # main page
├── css/
│   └── style.css           # main stylesheet (responsive)
├── js/
│   ├── app.js              # application entry point
│   ├── websocket.js        # WebSocket client
│   ├── streams.js          # stream management logic
│   └── subscriptions.js    # subscription management
└── assets/
    └── icons/              # icon assets
```

## Usage

### Connection management

- Click the "Connect" button to connect manually
- Click the "Disconnect" button to disconnect
- Reconnects automatically after a disconnect (up to 5 times)

### Stream management

- The left panel lists all streams
- Click a stream to view its details
- Use the search box to filter streams
- Click "+ New Stream" to create a stream

### Content editing

- The "Content" tab shows the stream content
- Use the toolbar buttons to perform operations
- Use the search box to search the content
- Click a search result to jump to the matching position

### Subscription management

- The "Subscriptions" tab manages subscriptions
- Click the "Subscribe" button to subscribe to a stream
- Change notifications appear in real time
- Click "Unsubscribe" to cancel a subscription

### Metadata management

- The "Metadata" tab manages metadata
- Edit the description and click "Update"
- Add or remove tags
- Rename a stream

## Tech Stack

- HTML5
- CSS3 (CSS Grid/Flexbox)
- ES6+ JavaScript
- WebSocket API

## Browser Support

- Chrome 60+
- Firefox 55+
- Safari 11+
- Edge 79+

## Development

No build step is required; edit the files directly.

### Debugging

Open the browser developer tools (F12) and check the console logs.

### Extending

To add new features:

1. Add the logic in the corresponding JS file
2. Add the UI elements in the HTML
3. Add the styles in the CSS
4. Wire up the events in app.js

## FAQ

### Connection fails

- Check that the text stream system is running
- Check that the port is correct
- Check the firewall settings

### Authentication fails

- Check that the token is correct
- Check whether authentication is enabled on the system

### Operations fail

- Check that the stream exists
- Check for write-lock conflicts
- Check the console error messages

## License

Apache-2.0, see [LICENSE](../../LICENSE) at the repository root.
