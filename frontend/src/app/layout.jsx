import './globals.css'

export const metadata = {
  title: 'Order to Cash — Graph Explorer',
  description: 'Visualize and query your O2C process graph',
}

export default function RootLayout({ children }) {
  return (
    <html lang="en">
      <body>{children}</body>
    </html>
  )
}