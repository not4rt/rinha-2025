#!/usr/bin/env python3
import http.server
import socketserver
import json
from urllib.parse import parse_qs

class DebugHTTPRequestHandler(http.server.BaseHTTPRequestHandler):
    def do_GET(self):
        self.handle_request()
    
    def do_POST(self):
        self.handle_request()
    
    def do_PUT(self):
        self.handle_request()
    
    def do_DELETE(self):
        self.handle_request()
    
    def do_PATCH(self):
        self.handle_request()
    
    def do_HEAD(self):
        self.handle_request()
    
    def do_OPTIONS(self):
        self.handle_request()
    
    def handle_request(self):
        print(f"\n{'='*80}")
        print(f"NEW REQUEST")
        print(f"{'='*80}")
        
        print(f"Method: {self.command}")
        print(f"Path: {self.path}")
        print(f"HTTP Version: {self.request_version}")
        print(f"Client Address: {self.client_address}")
        
        print(f"\nHeaders:")
        for header, value in self.headers.items():
            print(f"  {header}: {value}")
        
        content_length = int(self.headers.get('Content-Length', 0))
        if content_length > 0:
            body = self.rfile.read(content_length)
            print(f"\nBody ({content_length} bytes):")
            try:
                decoded_body = body.decode('utf-8')
                print(f"  {decoded_body}")
                
                if self.headers.get('Content-Type', '').startswith('application/json'):
                    try:
                        json_data = json.loads(decoded_body)
                        print(f"  Parsed JSON: {json.dumps(json_data, indent=2)}")
                    except json.JSONDecodeError:
                        print("  (Invalid JSON)")
            except UnicodeDecodeError:
                print(f"  (Binary data: {body[:100]}...)")
        
        query_params = parse_qs(self.path.split('?', 1)[1] if '?' in self.path else '')
        if query_params:
            print(f"\nQuery Parameters:")
            for key, values in query_params.items():
                print(f"  {key}: {values}")
        
        print(f"{'='*80}\n")
        
        self.send_response(200)
        self.send_header('Content-type', 'text/plain')
        self.end_headers()
        self.wfile.write(b'Request logged successfully\n')

if __name__ == "__main__":
    PORT = 8000
    
    with socketserver.TCPServer(("", PORT), DebugHTTPRequestHandler) as httpd:
        print(f"Debug HTTP Server running on port {PORT}")
        print(f"Visit http://localhost:{PORT} to test")
        print("Press Ctrl+C to stop")
        
        try:
            httpd.serve_forever()
        except KeyboardInterrupt:
            print("\nServer stopped.")