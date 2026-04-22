/**Proxy: Get current user info*/
import { NextRequest, NextResponse } from 'next/server';
import axios from 'axios';

const API_BASE_URL = process.env.NEXT_PUBLIC_API_URL || 'http://localhost:8000';

export async function GET(request: NextRequest) {
  try {
    const accessToken = request.cookies.get('access_token')?.value;

    if (!accessToken) {
      return NextResponse.json({ error: 'No token' }, { status: 401 });
    }

    // Call backend /me endpoint with token
    const response = await axios.get(`${API_BASE_URL}/api/v4/auth/me`, {
      headers: {
        Authorization: `Bearer ${accessToken}`,
      },
    });

    return NextResponse.json(response.data, { status: 200 });
  } catch (error: any) {
    const status = error.response?.status || 401;
    return NextResponse.json({ error: 'Unauthorized' }, { status });
  }
}
